package com.oconnors.suggest.nphrases.parse;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.ServerConnectionException;
import com.oconnors.suggest.nphrases.comm.Queryer;

import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
import edu.stanford.nlp.process.CoreLabelTokenFactory;
import edu.stanford.nlp.process.PTBTokenizer;
import edu.stanford.nlp.process.TokenizerFactory;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreePrint;

/**
 * Convenience class for parsing documents
 * @author kford
 *
 */
public class DocumentParse {
	
	final static Logger logger = LogManager.getLogger(DocumentParse.class.getName());

	/**names of phrase types: nphrase1, nphrase2, and vphrase1*/
	private static final String[] PHRASE_NAMES = new String[]{"nphrase1", "nphrase2", "vphrase1"};
	/**max number of words in a sentence before it is split into clauses for parsing*/
	private final int MAX_SENTENCE_LENGTH = 50; 

	/** prints trees as xml strings*/
	private static final TreePrint TREE_PRINT = new TreePrint("xmlTree");
	/** parser object, using English PCFG grammar*/
	private static final LexicalizedParser LEX_PARSE = LexicalizedParser.loadModel("englishPCFG.ser.gz");
	/**factory for generating tokenizers*/
	private static final TokenizerFactory<CoreLabel> TOKENIZER_FACT = PTBTokenizer.factory(new CoreLabelTokenFactory(),
			"normalizeAmpersandEntity=false,normalizeOtherBrackets=false,asciiQuotes=true,latexQuotes=false,untokenizable=noneDelete");
	/** Maximum entropy tagger used to tag the sentence before parsing it*/
	private static final MaxentTagger TAGGER = new MaxentTagger("models/tagger/english-left3words-distsim.tagger");
	
	/**filename of document being processed*/
	private String filename = "";
	/**product containing document being processed*/
	private String product = "";
	private String edition = "";

	/**Queryer object used to execute xquery code on the MarkLogic server*/
	private Queryer queryer;
  
  private String id = "";

	/**number of batches that have finished generating and inserting nphrases*/
	private AtomicInteger batchesFinished;
	/**total number of batches spawned for this document*/
	private int batchesCount;

	/**threadpool handling this task*/
	private final ThreadPoolExecutor threadpool;
  
  private final ConcurrentHashMap<String, DocumentParse> docparses;
	
	/**shutdown flag: when set to true, attempts to shut down gracefully*/
	private AtomicBoolean shutdown;
	

	/**
	 * Constructs a new DocumentParse that will get a document, parse it, generate 
	 * nphrases for it, and insert those nphrases into the database
	 * @param uri			uri of the document to process
	 * @param product		product containing the document to process
	 * @param queryer		Queryer used to execute xquery code on the MarkLogic server
	 * @param threadpool	threadpool handling this task
	 * @throws FileNotFoundException 
	 */
	public DocumentParse(String uri, String product, String edition, Queryer queryer,
    ThreadPoolExecutor threadpool, ConcurrentHashMap<String, DocumentParse> docparses){
		
		this.filename = uri;
		this.product = product;
		this.edition = edition;

		this.queryer = queryer;
    this.docparses = docparses;
    
    this.id = filename + product + edition + queryer.getXccUrl();

		batchesFinished = new AtomicInteger(0);

		this.threadpool = threadpool;
		
		shutdown = new AtomicBoolean(false);
		
	}
	

	/**
	 * Get a batch of the document's paragraphs, parse it, and generate and insert nphrases
	 * @param token		authorization token
	 * @param batchId	batch number to process
	 * @throws ServerConnectionException  only when connection refused: indicates possible problem with Queryer's xccUrl
	 */
	public void process(final String token, final int batchId) throws ServerConnectionException {

		try {
			if (!shutdown.get())
				parse(getSens(token, batchId), token, batchId);
      synchronized (batchesFinished) {
        log("finished " + (batchesFinished.get() + 1) + "/" + batchesCount + " batches", 5);
        if (batchesFinished.incrementAndGet() == batchesCount){
          log("finished: " + batchesFinished.get() + " (left in threadpool: " + threadpool.getQueue().size() + ")", 5);
          if (threadpool.getQueue().size()==0 && threadpool.getActiveCount()==1){
            log("finished all queued tasks.", 5);
            System.gc();
          }
        }
      }
		} catch (ServerConnectionException e){
			if (e.getMessage().contains("Connection refused")){ 
				throw e;
			} else {
				log("Request Exception: " + e.getMessage(), 10);
				e.printStackTrace();
			}
		} catch (RequestException e) {
			log("Request Exception: " + e.getMessage(), 10);
			e.printStackTrace();
			shutdown.set(true);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
      if (batchesFinished.get() >= batchesCount || shutdown.get()){
        synchronized (docparses) {
          synchronized (id) {
            log("Docparse finished: " + id, 3);
            if (docparses.containsKey(id))
              docparses.remove(id);
            else 
              log("***docparses doesn't contain key: " + id, 10);
          }
        }
      }
    }
	}

	/**
	 * Gets the sentences for this batch of paragraphs
	 * @param token		authorization token
	 * @param batchId	batch number
	 * @return a ResultSequence of sentences 
	 * @throws RequestException
	 * @throws IOException
	 */
	private ResultSequence getSens(final String token, final int batchId) 
			throws RequestException, IOException{
		ResultSequence rs_sens;

		//get the sentences in the batch
		rs_sens = queryer.documentGet(product, edition, filename, token, batchId);
		if (batchId == 0){ //spawn new tasks for the other batches
			batchesCount = Integer.parseInt(rs_sens.next().asString());
			log("batches count: " + batchesCount, 4);
			for (int batch=1; batch<batchesCount; batch++){
				final int nextBatch = batch;
				log("spawning new task for batch " + batch, 5);
				threadpool.execute(new Runnable(){
					public void run(){
						//get the document, parse it into sentences, batch the sentences
						try {
							process(token, nextBatch);
						} catch (ServerConnectionException e) {
							//if this exception occurs here, at least one connection attempt
							//has already succeeded, so the problem is with the connection
							//or the server, not the Queryer's xccUrl
							log("Request Exception: " + e.getMessage(), 10);
							e.printStackTrace();
						}
					}
					
				});
			}
		}

		log("batch size : " + (rs_sens.size()-1) + " sens | " + rs_sens.asString().getBytes().length + " bytes", 2);
		return rs_sens;
	}

	/**
	 * Parse the given sentences, generate nphrases from their parse trees, and
	 * insert the nphrases into the database
	 * @param batch		ResultSequence of sentences
	 * @param token		authorization token
	 * @param batchId	batch number
	 * @throws IOException
	 * @throws RequestException
	 */
	public void parse(ResultSequence batch, String token, int batchId) throws IOException, RequestException {
		
		log("parsing for batch " + batchId + " (" + batch.size() + " sentences)", 2);
		long parse_time_start, parse_time_end;
		parse_time_start = System.nanoTime();

		ByteArrayOutputStream tree_out = new ByteArrayOutputStream();
		PrintWriter tree_writer = new PrintWriter(tree_out);

		//parse each sentence and write its parse tree to tree_writer
		System.out.println("batch " + batchId + ": parsing");
		while (batch.hasNext()){
			if (shutdown.get()){
				log("batch " + batchId + " is shutting down", 5);
				return; 
			}
			parseMaxLength(batch.next().asString(), tree_writer);
		}
		System.out.println("batch " + batchId + ": parsed");
		parse_time_end = System.nanoTime();
		log("parsed in :" + (parse_time_end - parse_time_start) / 1000000000.0 + "s.", 5);

		//get the parse trees
		tree_writer.close();
		String xmlTree = tree_out.toString();
		tree_out.flush();
		String trees = "<SENS>" + DocUtils.fixXML(xmlTree) + "</SENS>";

		//generate and insert the nphrases
		generateNPhrases(token, batchId, trees);
	}

	/**
	 * Generates and inserts nphrases for a batch of parse trees
	 * @param token		authorization token
	 * @param batchID	batch number being processed
	 * @param trees		parse trees, in properly formatted XML
	 * @throws RequestException
	 * @throws IOException
	 */
	private void generateNPhrases(String token, int batchId, String trees) throws IOException, RequestException{

		if (shutdown.get()) { 
			log("batch " + batchId + " is shutting down", 5);
			return; 
		}
		long time_start = System.nanoTime();
		ResultSequence rs = queryer.doNPhrases(trees, product, edition, filename, ""+batchId, token);
		long time_end = System.nanoTime();


		//log nphrase data
		int[] phraseCounts = new int[PHRASE_NAMES.length];
		for (int i=0; i < PHRASE_NAMES.length; i++){
			phraseCounts[i] = Integer.parseInt(rs.itemAt(i).asString());
		}
		int sum = 0;
		for (int i=0; i < phraseCounts.length; i++){
			sum += phraseCounts[i];
		}
		log("generated & inserted " + sum + " nphrases (" + phraseCounts[0] + ", " +
				phraseCounts[1] + ", " + phraseCounts[2] + ") in batch " + batchId + " in " + (time_end - time_start) / 1000000000.0 + "s.", 5);


	}

	/**
	 * If sentences are over MAX_SENTENCE_LENGTH words long, then split them at the commas and 
	 * parse the fragments. Otherwise parse the sentences.
	 * @param text			A single sentence
	 * @param tree_writer 	Writer that parse trees are written to
	 */
	private void parseMaxLength(String text, PrintWriter tree_writer){

		String words[] = text.split(" ");
		if (words.length >= MAX_SENTENCE_LENGTH){
			for (String clause : text.split(",")){
				stanfordParse(clause, tree_writer);
			}
		} else {
			stanfordParse(text, tree_writer);
		}
	}

	/** Get a parse tree of this sentence using the Stanford parser 
	 * Uses the Stanford tagger to tag the sentence before parsing
	 * Parse trees are written to tree_writer and can be retrieved by closing tree_writer
	 * and calling toString() on the associated output stream
	 * @param text 			A single sentence or fragment
	 * @param tree_writer	Writer that parse trees are written to
	 **/
	private void stanfordParse(String text, PrintWriter tree_writer){
		StringReader sent2Reader = new StringReader(text);
		List<CoreLabel> tokens = TOKENIZER_FACT.getTokenizer(sent2Reader).tokenize();
		List<TaggedWord> tags = TAGGER.apply(tokens);
		Tree parse = LEX_PARSE.apply(tags);
		TREE_PRINT.printTree(parse, tree_writer);

	}


	/**
	 * print message to log file
	 * @param msg		message to log
	 */
	public void log(String msg, int debugLevel){
			msg = " (" + threadpool.getActiveCount() + 
					" active) | " + filename + " | " + msg;
			
			if (debugLevel > 8){
				logger.error(msg);
			} else if (debugLevel > 5){
				logger.warn(msg);
			} else if (debugLevel >= 3){
				logger.info(msg);
			} else{
				logger.trace(msg);
			}
	}
  
  public void shutdown(){
    synchronized (id) {
      shutdown.set(true);
      id = id + "-shutdown";
    }
  }

}