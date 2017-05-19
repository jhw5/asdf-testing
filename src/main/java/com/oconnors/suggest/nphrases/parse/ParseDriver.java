package com.oconnors.suggest.nphrases.parse;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.marklogic.xcc.exceptions.ServerConnectionException;
import com.oconnors.suggest.nphrases.comm.Queryer;

public class ParseDriver {

	/**thread pool handling requests from ML*/
	private static ThreadPoolExecutor threadpool = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors(),
			Runtime.getRuntime().availableProcessors(), 1, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>());
	/**Maps xcc url to Queryer: one database connection per server url*/
	private static HashMap<String,Queryer> queryers = new HashMap<String,Queryer>();
  
  private final static ConcurrentHashMap<String, DocumentParse> docparses = new ConcurrentHashMap<String, DocumentParse>();
		
	final static Logger logger = LogManager.getLogger(ParseDriver.class.getName());


	/**
	 *  Parses config data into parameters for DocumentParse and processes a document
	 * @param filename		filename of document for which to generate nphrases
	 * @param product	product containing document
	 * @param xccUrl	address:port of MarkLogic 
	 * @param token		authorization token
	 * @param paths		array of real paths to xquery source files and java config file
	 * @throws IOException
	 */
	/*use local config data*/
	public static void process(String filename, String product, String edition, String xccCredentials, String xccUrl, String token, String[] paths) throws IOException{
    synchronized (queryers) {
      if (!queryers.containsKey(xccUrl)){
        logger.info("Created Queryer for: " + xccUrl);
        queryers.put(xccUrl, new Queryer(xccCredentials, xccUrl, paths));
      }
    }
		if (product.equals("") || filename.equals("")){
			throw new IllegalArgumentException("No uri or product specified!");
		}
		
		if (!threadpool.allowsCoreThreadTimeOut()){
			threadpool.allowCoreThreadTimeOut(true);
		}

		spawnTask(filename, product, edition, xccUrl, token);
	}

	/**
	 * Gets a document from the database, parses it into sentences, and generates and inserts nphrases from those sentences
	 * @param filename		filename of document for which to generate nphrases
	 * @param product	product containing document
	 * @param queryer	used to query MarkLogic via XCC
	 * @param token		authorization token
	 * @throws FileNotFoundException 
	 */
	public static void spawnTask(final String filename, final String product, final String edition, String xccUrl, final String token){
		logger.info("spawning task for document: " + filename);
		final Queryer queryer = queryers.get(xccUrl);
    final String docparseID = filename + product + edition + xccUrl;
    synchronized (docparses){
      if (docparses.containsKey(docparseID)) {
        docparses.get(docparseID).shutdown();
        docparses.remove(docparseID);
      } 
      docparses.put(docparseID, new DocumentParse(filename, product, edition, queryer, threadpool, docparses));
    }
		final DocumentParse docparse = docparses.get(docparseID);
		threadpool.execute(new Runnable(){
			public void run(){
				try {
					logger.info("parsing document: " + filename);
					//get the document, parse it, and generate & insert nphrases
					docparse.process(token, 0);
				} catch (ServerConnectionException e) {
					//indicates a possible problem with Queryer's xccUrl: delete database connection 
					//reaches this point only when batch 0 fails: i.e., there have been no successful connection attempts
					if (e.getMessage().contains("Connection refused")) {
						logger.warn("removing Queryer: " + queryer.getXccUrl());
						queryers.remove(queryer.getXccUrl());
					}
					e.printStackTrace();
				}
			}
			
		});
	}
  
  /**
  * Returns true if the threadpool has active tasks
  */
  public static boolean status(){
    logger.info("Current DocumentParses: ");
    for (String docparseID : docparses.keySet()) {
      logger.info(docparseID);
    }
    if (threadpool.getActiveCount() > 0) {
      return true;
    }
    return false;
  }

	/**
  *  Wait for all tasks to finish before shutting down; do not accept new tasks
  */
	public static void shutdown(){
		logger.info("Shutting down...");
		try {
			threadpool.shutdown();
			while (!threadpool.awaitTermination(20, TimeUnit.SECONDS)){
				logger.trace("not finished: " + threadpool.getActiveCount() + " still running.");
			}
			logger.info("all tasks terminated");
		} catch (InterruptedException e) {
			// wait for shutdown to finish
		}
	}
  
  /**
  *  Shuts down all active document tasks without waiting for them to finish.
  *  Does not prevent accepting new tasks
  */
  public static void shutdown_all(){
    for (String docparseID : docparses.keySet()){
      docparses.get(docparseID).shutdown();
      docparses.remove(docparseID);
    }
  }
  
  /**
  *  Shut down a specific task identified by its request parameters
  */
  public static void shutdown(String filename, String product, String edition, String xccUrl){
		logger.info("Shutting down "  + filename);
    String docparseID = filename + product + edition + xccUrl;
    if (docparses.containsKey(docparseID)) {
      docparses.get(docparseID).shutdown();
    }
	}


}
