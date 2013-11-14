/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.dataetl;

import cn.ac.iie.cls.cc.slave.SlaveHandler;
import cn.ac.iie.cls.cc.slave.clsagent.DataCollectJob;
import cn.ac.iie.cls.cc.slave.clsagent.DataCollectJobTracker;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author alexmu
 */
public class DataETLExecuteHandler implements SlaveHandler {

    private static final String SUCCESS_RESPONSE = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?><response><processJobInstanceId>PJID</processJobInstanceId><status>SUCCEED</status></response>";
    private static final String FAIL_RESPONSE = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?><error><processJobInstanceId>PJID</processJobInstanceId><status>FAILED</status><message>MESSAGE<message></error>";
    
    @Override
    public String execute(String pRequestContent) {
        String result = null;

        ETLJob etlJob = ETLJob.getETLJob(pRequestContent,true);

        if (etlJob != null) {
            String clsAgentDataCollectDescriptor = etlJob.getDataProcessDescriptor().get(ETLJob.CLS_AGENT_DATA_COLLECT_DESC);
            System.out.println(clsAgentDataCollectDescriptor);
            if (clsAgentDataCollectDescriptor != null) {
                DataCollectJob dataCollectJob = DataCollectJob.getDataCollectJob(clsAgentDataCollectDescriptor);
                dataCollectJob.setEtlJob(etlJob);
                DataCollectJobTracker.getDataCollectJobTracker().appendJob(dataCollectJob);
            }

            ETLJobTracker.getETLJobTracker().appendJob(etlJob);
            if (clsAgentDataCollectDescriptor == null) {
                String inputFilePath = etlJob.getInputFilePathStr();
                System.out.println("#####inputFilePath:" + inputFilePath);
                List<ETLTask> etlTaskList = new ArrayList<ETLTask>();
                etlTaskList.add(new ETLTask(inputFilePath, ETLTask.EXECUTING));
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
