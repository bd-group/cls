/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.master;

import cn.ac.iie.cls.cc.slave.SlaveHandler;
import cn.ac.iie.cls.cc.slave.SlaveHandlerFactory;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

/**
 *
 * @author mwm
 */
public class CCHandler extends AbstractHandler {
    
    private static CCHandler ccHandler = null;
    static Logger logger = null;
    
    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(CCHandler.class.getName());
    }
    
    public static CCHandler getCCHandler() {
        if (ccHandler != null) {
            return ccHandler;
        }
        ccHandler = new CCHandler();
        return ccHandler;
    }
    
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        
        baseRequest.setHandled(true);
        
        String requestPath = baseRequest.getPathInfo().toLowerCase();
        logger.debug(requestPath);
        
        ServletInputStream servletInputStream = baseRequest.getInputStream();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] b = new byte[4096];
        int i = 0;
        while ((i = servletInputStream.read(b, 0, 4096)) > 0) {
            out.write(b, 0, i);
        }
        String requestContent = new String(out.toByteArray(), "UTF-8");
        logger.debug(requestContent);
        
        try {
            SlaveHandler slaveHandler = SlaveHandlerFactory.getSlaveHandler(requestPath);
            
            response.setStatus(HttpServletResponse.SC_OK);
            logger.info(requestContent);
            response.getWriter().println(slaveHandler.execute(requestContent));
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
        }
        
    }
}
