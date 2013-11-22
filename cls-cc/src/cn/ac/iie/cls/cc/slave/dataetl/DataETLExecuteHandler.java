/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.dataetl;

import cn.ac.ict.ncic.util.dao.Dao;
import cn.ac.ict.ncic.util.dao.DaoPool;
import cn.ac.iie.cls.cc.commons.RuntimeEnv;
import cn.ac.iie.cls.cc.slave.SlaveHandler;
import cn.ac.iie.cls.cc.slave.clsagent.DataCollectJob;
import cn.ac.iie.cls.cc.slave.clsagent.DataCollectJobTracker;
import static cn.ac.iie.cls.cc.slave.dataetl.ETLJob.logger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author alexmu
 */
public class DataETLExecuteHandler implements SlaveHandler {

    private static final String SUCCESS_RESPONSE = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?><response><processJobInstanceId>PJID</processJobInstanceId><status>SUCCEED</status></response>";
    private static final String FAIL_RESPONSE = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?><error><processJobInstanceId>PJID</processJobInstanceId><status>FAILED</status><message>MESSAGE<message></error>";
    
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(ETLJob.class.getName());
    }
    
    @Override
    public String execute(String pRequestContent) {
        String result = null;
        ResultSet rs = null;
        ETLJob etlJob = ETLJob.getETLJob(pRequestContent);

        if (etlJob != null) {
            String clsAgentDataCollectDescriptor = etlJob.getDataProcessDescriptor().get(ETLJob.CLS_AGENT_DATA_COLLECT_DESC);
             String clsETLDataCollectDescriptor = etlJob.getDataProcessDescriptor().get(ETLJob.DATA_ETL_DESC);
            
            Dao dao = DaoPool.getDao(RuntimeEnv.METADB_CLUSTER);
        	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        	String date = sdf.format(new Date());
            try {
            	String sql = "insert into dp_job values ('" + etlJob.getProcessJobInstanceID() + "', '" + clsAgentDataCollectDescriptor + "', '" + clsETLDataCollectDescriptor + "','" + ETLJob.JobStatus.QUEUING + "',0, to_date('" + date + "', 'YYYY-MM-DD HH24:MI:SS'))";
                logger.info(sql);
                logger.info("job inserted into DB: etl job " + etlJob.getProcessJobInstanceID() + " has inserted into dca_task successfully");
                dao.executeUpdate(sql);
            } catch (Exception ex) {
            	ex.printStackTrace();
            }
            
            Connection tmpConn = null;
            try {
                tmpConn = dao.getConnection();
                tmpConn.close();
            } catch (Exception ex) {
            	logger.warn("errors exists in closing sql connection", ex);
            }
            System.out.println(clsAgentDataCollectDescriptor);
            if (clsAgentDataCollectDescriptor != null) {
                DataCollectJob dataCollectJob = DataCollectJob.getDataCollectJob(clsAgentDataCollectDescriptor);
                dataCollectJob.setEtlJob(etlJob);
                DataCollectJobTracker.getDataCollectJobTracker().appendJob(dataCollectJob);
            }

            ETLJobTracker.getETLJobTracker().appendJob(etlJob);
            if (clsAgentDataCollectDescriptor == null) {
                String inputFilePath = etlJob.getInputFilePathStr();
                String taskId = UUID.randomUUID().toString();
                System.out.println("#####inputFilePath:" + inputFilePath);
                List<ETLTask> etlTaskList = new ArrayList<ETLTask>();
                etlTaskList.add(new ETLTask(inputFilePath, ETLTask.ETLTaskStatus.EXECUTING, taskId));
                etlJob.setTask2doNum(1);
                etlJob.appendTask(etlTaskList);
            }
            result = SUCCESS_RESPONSE.replace("PJID", etlJob.getProcessJobInstanceID());
        } else {
            result = FAIL_RESPONSE.replace("PJID", etlJob.getProcessJobInstanceID()).replace("MESSAGE", "init etl task failed for ...");
        }

        return result;
    }
}
