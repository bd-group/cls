/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.clsagent;

import cn.ac.iie.cls.cc.slave.SlaveHandler;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author alexmu
 */
public class CLSAgentReportHandler implements SlaveHandler {
    
    static Logger logger = null;
    
    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(CLSAgentReportHandler.class.getName());
    }
    
    public String execute(String pRequestContent) {
        String xml = "";
        xml = pRequestContent;
        if (xml.equals("")) {
            return "CLSAgentReportHandler's pRequestContent is empty!";
        }
        logger.info("response from cls-agent " + xml);

        //file which has been upload
        if (xml.startsWith("database")) {
            logger.info("database: " + xml);
            String[] splitXml = xml.split("[|]");
            String processInstanceID = "";
            processInstanceID = splitXml[3];
            List<DataCollectTask> dctList = new ArrayList<DataCollectTask>();
            
            
            if (splitXml[1].equals("all")) {//all
                boolean doTask = false;
                for (int i = 0; i < splitXml.length; i++) {
                    if (i > 5) {
                        String type = splitXml[2];
                        if (type.equals("GetherDataFromThrid")) {
                            logger.info("#######total: " + splitXml[i]);
                        } else {
                            DataCollectTask dct = new DataCollectTask(splitXml[i]);
                            dct.taskStatus = DataCollectTask.EXECUTING;
                            if(!splitXml[i].equals("is not a dir")){
                                dctList.add(dct);
                            }else{
                                logger.warn("this is not a dir,please check path or date.");
                            }
                            //dctList.add(dct);
                            logger.info("#######total: " + dct.fileName);
                            DataCollectJobTracker.getDataCollectJobTracker().appendTask(processInstanceID, dctList);
                            doTask = true;
                        }
                    }
                }
                if (!doTask) {
                    DataCollectJobTracker.getDataCollectJobTracker().appendTask(processInstanceID, dctList);
                }
            } else if (splitXml[1].equals("one")) {//one
                for (int i = 0; i < splitXml.length; i++) {
                    if (i > 5) {
                        String type = splitXml[2];
                        if (type.equals("GetherDataFromThrid")) {
                            logger.info("#######one: " + splitXml[i]);
                        } else {
                            DataCollectTask dct = new DataCollectTask(splitXml[i]);
                            dct.taskStatus = DataCollectTask.SUCCEEDED;
                            dctList.add(dct);
                            logger.info("#######one: " + dct.fileName);
                            DataCollectJobTracker.getDataCollectJobTracker().responseTask(processInstanceID, dctList);
                        }
                    }
                }
                
            } else if (splitXml[1].equals("err")) {//one
                for (int i = 0; i < splitXml.length; i++) {
                    if (i > 5) {
                        String type = splitXml[2];
                        if (type.equals("GetherDataFromThrid")) {
                            logger.info("#######err: " + splitXml[i]);
                        } else {
                            DataCollectTask dct = new DataCollectTask(splitXml[i]);
                            dct.taskStatus = DataCollectTask.FAILED;
                            dctList.add(dct);
                            logger.info("#######err: " + dct.fileName);
                            DataCollectJobTracker.getDataCollectJobTracker().responseTask(processInstanceID, dctList);
                        }
                    }
                }
                
            } else {
                ;
            }
        } else if (xml.startsWith("status")) {
            if(xml.contains("return now")){
                List<DataCollectTask> dctList = new ArrayList<DataCollectTask>();
                String tmpprocessInstanceID = (xml.split("[|]"))[3];
                DataCollectJobTracker.getDataCollectJobTracker().appendTask(tmpprocessInstanceID, dctList);
            }
            if (xml.contains("heartbeat")) {
                logger.info("heartbeat log : " + (xml.split("[|]"))[1]);
            } else {
                logger.info("status log : " + (xml.split("[|]"))[1]);
            }
        }
        return "success!" + xml;
    }
}
