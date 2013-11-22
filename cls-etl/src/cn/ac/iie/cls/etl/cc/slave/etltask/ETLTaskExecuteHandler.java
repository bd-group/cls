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
        for (int i=0; i<10; i++) {
        	System.out.println("-----============new request + " + pRequestContent);
        }
        try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        logger.info(pRequestContent);
        ETLTask etlTask = ETLTask.getETLTask(pRequestContent);
        System.out.println("etlTask is " + etlTask);
        if (etlTask != null) {
            ETLTaskTracker.getETLTaskTracker().appendTask(etlTask);
            result = "ok";
        } else {
            result = "failed";
        }
        System.out.println("etlTask append " + result);
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
