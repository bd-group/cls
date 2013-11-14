/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.cc.slave.etltask;

import cn.ac.iie.cls.etl.cc.slave.SlaveHandler;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author alexmu
 */
public class ETLTaskExecuteHandler implements SlaveHandler {

    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(ETLTaskExecuteHandler.class.getName());
    }

    @Override
    public String execute(String pRequestContent) {
        String result = null;
        logger.info(pRequestContent);
        ETLTask etlTask = ETLTask.getETLTask(pRequestContent);
        if (etlTask != null) {
            ETLTaskTracker.getETLTaskTracker().appendTask(etlTask);
            result = "ok";
        } else {
            result = "failed";
        }
        return result;
    }

    public static void main(String[] args) {
        File inputXml = new File("tableOutputOperator-test-specific.xml");
        try {
            String dataProcessDescriptor = "";
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(inputXml)));
            String line = null;
            while ((line = br.readLine()) != null) {
                dataProcessDescriptor += line;
            }
            ETLTaskExecuteHandler etlTaskExecuteHandler = new ETLTaskExecuteHandler();
            System.out.println(etlTaskExecuteHandler.execute(dataProcessDescriptor));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
