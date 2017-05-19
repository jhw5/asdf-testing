package com.oconnors.suggest.nphrases.comm;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ContentSourceFactory;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccConfigException;
import com.marklogic.xcc.types.ValueType;

public class Queryer {
	
	final static Logger logger = LogManager.getLogger(Queryer.class.getName());
	
	/**Location of query source files in porject WAR */
	private static final String WAR_PREFIX = "/WEB-INF/classes/";
	
	//xquery source
	/** location of source code for a query that gets a batch of paragraphs from a document*/
	private static final String DOC_QUERY = "queries/document-get-sens.xqy";
	private String realDocQueryPath;
	/**location of source code for a query that generates nphrases from parse trees and
	 * inserts them into the database*/
	private static final String NPHRASES_QUERY = "queries/nphrases.xqy";
	private String realNPhraseQueryPath;

	/**caches loaded query code*/
	private HashMap<String, String> queryMap;

	/** describes the current MarkLogic server instance and serves as a factory that 
	 * creates session objects*/
	private ContentSource contentSource;
	
	/**URL for MarkLogic server on which to execute queries*/
	private String xccUrl;

	/**
	 * Constructs a new Queryer
	 * @param xccUser		credentials of MarkLogic user: username:password
	 * @param xccUrl 		url of MarkLogic server: addr:port
	 * @param context		context of the servlet that received the request: used
	 * 						 to find query source files in the deployed webapp
	 * @throws RequestException
	 */
	public Queryer(String xccUser, String xccUrl, String[] paths)  {
		String serverUri;
		this.xccUrl = xccUrl;
		if (!xccUser.startsWith("xcc://")){
			serverUri = "xcc://" + xccUser + "@" + xccUrl; 
		} else {
			serverUri = xccUser + "@" + xccUrl;
		}
		realDocQueryPath = paths[0];
		realNPhraseQueryPath = paths[1];
		queryMap = new HashMap<String, String>(5);
		URI serverURI;
		try {
			serverURI = new URI(serverUri);
			contentSource = ContentSourceFactory.newContentSource(serverURI);
		} catch (URISyntaxException e) {
			e.printStackTrace();
		} catch (XccConfigException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Gets the address and port of the ML server this queryer is connecting to.
	 * Format: addr:port
	 * @return this queryer's ML server address
	 */
	public String getXccUrl() {
		return xccUrl;
	}
	

	/**
	 * Loads the text of an xqy script and executes it using XCC
	 * @param querypath		filename of script to execute
	 * @param vars			names of external variables of xqy script
	 * @param varTypes		types of external variables,
	 * 						 where each entry corresponds to 
	 * 						 the entry at the same index in vars
	 * @param values		values of each external variable, where each 
	 * 						 entry is of the corresponding varType and 
	 * 						 corresponds to the the entry at the same index in vars
	 * @return The results of the query, as a ResultSequence
	 * @throws IOException
	 * @throws RequestException
	 */
	public ResultSequence runQuery(String querypath, String[] vars, ValueType varTypes[], Object[] values)
			throws IOException, RequestException{

		if (vars.length != varTypes.length || vars.length != values.length) {
			throw new IllegalArgumentException("Arrays vars, varTypes, and values must all have the same length");
		}
		ResultSequence rs;
		Session session = contentSource.newSession();
		String query = loadQuery(querypath);
		Request req = session.newAdhocQuery(query);
		for (int i=0; i < vars.length; i++){
			req.setNewVariable(vars[i], varTypes[i], values[i]);
		}
		rs = session.submitRequest(req);
		session.close();
		
		if (rs.size()==2 && rs.itemAt(0).asString().equals("ERROR")){
			throw new RequestException(rs.itemAt(1).asString(), req);
		}
		
		return rs;

	}
	
	/**
	 * Convenience method for running a query that has only 1 external variable
	 * @param querypath
	 * @param var			the name of the external variable
	 * @param varType		the type of the external variable 
	 * @param value			the value of the external variable
	 * @return				The results of running the query in MarkLogic
	 * @throws IOException
	 * @throws RequestException
	 */
	public ResultSequence runQuery(String querypath, String var, ValueType varType, Object value)
			throws IOException, RequestException{
		
		return runQuery(querypath, new String[]{var}, new ValueType[]{varType}, new Object[]{value});
		
	}
	
	/* Convenience methods for specific queries */
	/**
	 * Gets a batch of the specified document's sentences
	 * @param fileuri	the URI of the document in the database
	 * @param token		authorization token
	 * @param batchId	batch number being requested
	 * @return			A ResultSequence whose first entry is the batch id of the next batch of sentences
	 * 					 or -1 if this was the last batch, 
	 * 					and whose other entries are sentences from the document as <p> elements
	 * @throws RequestException
	 * @throws IOException
	 */
	public ResultSequence documentGet(String product, String edition, String filename, String token, int batchId) throws RequestException, IOException{

		return runQuery(realDocQueryPath, new String[]{"product", "edition", "filename", "token", "batch-id"}, 
				 new ValueType[]{ValueType.XS_STRING, ValueType.XS_STRING, ValueType.XS_STRING, ValueType.XS_STRING, ValueType.XS_INTEGER},
				 new Object[]{product, edition, filename, new String(token), batchId});

	}
	
	/**
	 * Generates nphrases from parse trees and inserts them
	 * @param xmltree	Properly formatted xml containing this batches' parse trees
	 * @param uri		URI of document these nphrases are associated with
	 * @param product	product containing the document these nphrases are associated with
	 * @param batchId	batch number being processed
	 * @param token		authorization token
	 * @return			A ResultSequence whose first three entries are the respective counts
	 * 					 of the nphrase1, nphrase2, and vphrase1 objects, and whose remaining
	 * 					 three entries are <phrases> elements containing those objects
	 * @throws IOException
	 * @throws RequestException
	 */
	public ResultSequence doNPhrases(String xmltree, String product, String edition, String filename, String batchId, String token) 
			throws IOException, RequestException{
		
		return runQuery(realNPhraseQueryPath, new String[]{"docstr", "product", "edition", "filename", "batch-id", "token"}, 
				 new ValueType[]{ValueType.XS_STRING, ValueType.XS_STRING, ValueType.XS_STRING, ValueType.XS_STRING,
					ValueType.XS_STRING, ValueType.XS_STRING},
				 new Object[]{xmltree, product, edition, filename, batchId, token});
	
	}

	/**
	 * Load the source code of a query from file
	 * @param queryFile		filename of query file
	 * @return
	 * @throws IOException
	 */
	private String loadQuery(String queryFile) throws IOException{
	
		if (queryMap.containsKey(queryFile)){
			return queryMap.get(queryFile);
		}
		//only read in the file if not cached
		BufferedReader reader = new BufferedReader(new FileReader(queryFile));
		StringBuffer sb = new StringBuffer();
		String line;

		while ((line = reader.readLine()) != null) {
			sb.append(line).append("\n");
		}

		reader.close();

		String querytext = sb.toString();
		queryMap.put(queryFile, querytext);

		return querytext;
	}

	public static String[] getBasePaths() {
		return new String[]{WAR_PREFIX + DOC_QUERY, WAR_PREFIX + NPHRASES_QUERY};
	}

}
