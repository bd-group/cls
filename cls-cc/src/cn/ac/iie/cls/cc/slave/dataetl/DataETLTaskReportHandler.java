/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.dataetl;

import cn.ac.iie.cls.cc.slave.SlaveHandler;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author alexmu
 */
public class DataETLTaskReportHandler implements SlaveHandler {

    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(DataETLTaskReportHandler.class.getName());
    }

    public String execute(String pRequestContent) {
        String result = null;

        logger.info("response from cls-etl " + pRequestContent);
        
        String[] reportItems = pRequestContent.split("[|]");
        String processJobInstanceId = reportItems[2];
        String filePath = reportItems[3];
        //fixme
        ETLTask etlTask = new ETLTask(filePath, ETLTask.SUCCEEDED, reportItems[4]);
        List etlTaskList = new ArrayList<ETLTask>();
        etlTaskList.add(etlTask);
        ETLJobTracker.getETLJobTracker().responseTask(processJobInstanceId, etlTaskList);
        return result;
    }
}
