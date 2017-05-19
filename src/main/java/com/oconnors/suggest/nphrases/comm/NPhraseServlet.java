package com.oconnors.suggest.nphrases.comm;

import java.io.IOException;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.oconnors.suggest.nphrases.parse.ParseDriver;
import com.oconnors.suggest.nphrases.comm.Queryer;

/**
 * Servlet implementation class UriGetter
 */
@WebServlet("/NPhraseServlet")
public class NPhraseServlet extends HttpServlet {
	
	final static Logger logger = LogManager.getLogger(NPhraseServlet.class.getName());

	private static final long serialVersionUID = 5507600060595567531L;
	private ServletContext context;
	private boolean shutdown;

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public NPhraseServlet() {
		super();
	}

	@Override
	public void init(ServletConfig config) throws ServletException{
		super.init(config);
		context = config.getServletContext();
		shutdown = false;
	}

	@Override
	public void destroy(){
		ParseDriver.shutdown();
		super.destroy();
	}

	/**
	 * Handle GET request with parameters uri, product, xcc-url, and token
	 * uri: uri of document for which to generate nphrases
	 * product: product containing document
	 * xcc-url: address:port for MarkLogic 
	 * token: authorization token
	 * returns after queueing task to handle request, with no response
	 */
	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
 
		Map<String, String[]> parameters = request.getParameterMap(); 
    
    if (parameters.containsKey("status")) {
      logger.info("status request received");
      response.getWriter().print(ParseDriver.status());
      return;
    }

		if (shutdown){
			response.sendError(403, "Servlet is shutting down. This may be because the application is being redeployed.");
			return;
		}
		
		final String filename, product, edition, token, xccCredentials;
		String xccUrl;
		if (parameters.containsKey("filename")){
			filename = parameters.get("filename")[0];
		} else {
			response.sendError(400, "no filename specified");
			return;
		}
		if (parameters.containsKey("product")){
			product = parameters.get("product")[0];
		} else {
			response.sendError(400, "no product specified");
			return;
		}
		if (parameters.containsKey("edition")){
			edition = parameters.get("edition")[0];
		} else {
			response.sendError(400, "no edition specified");
			return;
		}
    if (parameters.containsKey("xcc-url")){
			xccUrl = parameters.get("xcc-url")[0];
			if (!xccUrl.contains(":")){ //just the port; use addr of request
				xccUrl = request.getRemoteAddr() + ":" + xccUrl;
			}
		} else {
			response.sendError(400, "no XCC URL specified");
			return;
		}
    if (parameters.containsKey("shutdown")){
      ParseDriver.shutdown(filename, product, edition, xccUrl);
    } else {
      if (parameters.containsKey("xcc-credentials")){
        xccCredentials = parameters.get("xcc-credentials")[0];
      } else {
        response.sendError(400, "no xcc credentials specified");
        return;
      }
      if (parameters.containsKey("token")){
        token = parameters.get("token")[0];
      } else {
        response.sendError(400, "no token given");
        return;
      }
      try {
        String[] queryPaths = Queryer.getBasePaths();
        String[] warPaths = new String[queryPaths.length];
        for (int i=0; i<queryPaths.length; i++){
          warPaths[i] = context.getRealPath(queryPaths[i]);
        }
        ParseDriver.process(filename, product, edition, xccCredentials, xccUrl, token, warPaths);
      } catch (IOException e) {
        logger.error(e.getMessage());
        response.sendError(500, e.getMessage());
      } 
    }
  }
}
