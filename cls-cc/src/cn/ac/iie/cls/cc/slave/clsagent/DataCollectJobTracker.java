/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.clsagent;

import cn.ac.iie.cls.cc.commons.RuntimeEnv;
import cn.ac.iie.cls.cc.slave.variablemanager.DateManager;
import cn.ac.iie.cls.cc.util.XMLReader;
import cn.ac.iie.cls.cc.util.ZooKeeperOperator;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author alexmu
 */
public class DataCollectJobTracker implements Runnable {

    private BlockingQueue<DataCollectJob> dataCollectJobWaitingList = new LinkedBlockingQueue<DataCollectJob>();
    private Map<String, DataCollectJob> executingDataCollectJobSet = new HashMap<String, DataCollectJob>();
    private Lock executingDataCollectJobSetLock = new ReentrantLock();
    private static DataCollectJobTracker dataCollectJobTracker = null;
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(DataCollectJobTracker.class.getName());
    }

    private DataCollectJobTracker() {
    }

    public static synchronized DataCollectJobTracker getDataCollectJobTracker() {
        if (dataCollectJobTracker == null) {
            dataCollectJobTracker = new DataCollectJobTracker();
            Thread dataCollectJobTrackerRunner = new Thread(dataCollectJobTracker);
            dataCollectJobTrackerRunner.start();
        }
        return dataCollectJobTracker;
    }

    public void appendJob(DataCollectJob pDataCollectJob) {
        try {
            dataCollectJobWaitingList.put(pDataCollectJob);
        } catch (Exception ex) {
        }
    }

    public void removeJob(DataCollectJob pDataCollectJob) {
        try {
            executingDataCollectJobSetLock.lock();
            executingDataCollectJobSet.remove(pDataCollectJob.getProcessJobInstanceID());
        } catch (Exception ex) {
            logger.warn("error happened " + ex.getMessage(), ex);
        } finally {
            executingDataCollectJobSetLock.unlock();
        }

    }

    public void appendTask(String pDataProcessInstanceId, List<DataCollectTask> pDataCollectTaskList) {
        DataCollectJob dataCollectJob = null;
        try {
            executingDataCollectJobSetLock.lock();
            dataCollectJob = executingDataCollectJobSet.get(pDataProcessInstanceId);
        } catch (Exception ex) {
            logger.warn("error happened " + ex.getMessage(), ex);
        } finally {
            executingDataCollectJobSetLock.unlock();
        }

        if (dataCollectJob == null) {
            //fixme
            logger.warn("can't find data process job with id " + pDataProcessInstanceId + ",maybe cls-cc has creashed before");
        } else {
            dataCollectJob.appendTask(pDataCollectTaskList);
        }
    }

    public void responseTask(String pDataProcessInstanceId, List<DataCollectTask> pDataCollectTaskList) {
        DataCollectJob dataCollectJob = null;
        try {
            executingDataCollectJobSetLock.lock();
            dataCollectJob = executingDataCollectJobSet.get(pDataProcessInstanceId);
        } catch (Exception ex) {
            logger.warn("error happened " + ex.getMessage(), ex);
        } finally {
            executingDataCollectJobSetLock.unlock();
        }

        if (dataCollectJob == null) {
            //fixme
            logger.warn("can't find data process job with id " + pDataProcessInstanceId + ",maybe cls-cc has creashed before");
        } else {
            dataCollectJob.responseTask(pDataCollectTaskList);
        }
    }

    @Override
    public void run() {
        DataCollectJob dataCollectJob = null;
        while (true) {
            try {
                dataCollectJob = dataCollectJobWaitingList.take();
                boolean succeeded = false;
                //dispatch
                //add by zy
                //retrive agent ip from data collect descriptor
                String host = "";
                int port = 7080;//fixme
                String dataCollectDesc = dataCollectJob.getDataProcessDescriptor();
                dataCollectDesc = DateManager.repalceDateWithSysTime(dataCollectDesc);//#{DATE,yyyymmdd}
                String agentIP = XMLReader.getValueFromStrDGText(dataCollectDesc, "agentIP");
                if (agentIP == null || agentIP.equals("")) {
                    logger.warn("abort data collect job for job" + dataCollectJob.getProcessJobInstanceID() + " for agent ip is not set in data collect descriptor \n" + dataCollectDesc);
                    dataCollectJob.appendTask(new ArrayList<DataCollectTask>());//make etl job exit
                    continue;
                } else {
                    host = agentIP;
                }

                //connect to zk to make sure current agent is online
                ZooKeeperOperator zkoperator = null;
                try {
                    zkoperator = new ZooKeeperOperator();
                    zkoperator.connect((String) RuntimeEnv.getParam(RuntimeEnv.ZK_CLUSTER));
                    if (!zkoperator.exists("/agent/" + agentIP).equals("true")) {
                        logger.warn("agent " + host + "is offline,so data collect job for job" + dataCollectJob.getProcessJobInstanceID() + " will wait for next try");
                        dataCollectJobWaitingList.put(dataCollectJob);
                        continue;
                    }
                } catch (Exception ex) {
                    logger.warn("connect to zookeeper cluster " + RuntimeEnv.ZK_CLUSTER + " unsuccessfully for " + ex.getMessage(), ex);
                    dataCollectJobWaitingList.put(dataCollectJob);
                    continue;
                } finally {
                    try {
                        zkoperator.close();
                    } catch (Exception ex) {
                    }
                }

                //dispatch data collect job to agent
                try {
                    HttpClient httpClient = new DefaultHttpClient();
                    HttpPost httppost = new HttpPost("http://" + host + ":" + port + "/resources/clsagent/execmd");

                    InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream(dataCollectDesc.getBytes()), -1);
                    reqEntity.setContentType("binary/octet-stream");
                    reqEntity.setChunked(true);
                    httppost.setEntity(reqEntity);
                    executingDataCollectJobSetLock.lock();
                    HttpResponse response = httpClient.execute(httppost);
                    //fixme:should dispose all kinds of condition
                    StatusLine sl = response.getStatusLine();
                    if (sl.getStatusCode() == 200) {
                        logger.info("dispatch data collect job for data process job " + dataCollectJob.getProcessJobInstanceID() + " to " + host + " successfully");
                        succeeded = true;
//                        String rs = HttpResponseParser.getResponseContent(response);
//                        if (rs.trim().equals("ok")) {
//                            logger.info("dispacth task for etl job " + processJobInstanceID + " of " + etlTask.filePath + " to " + etlTask.etlIpPort + " successfully");
//                            succeeded = true;
//                        } else {
//                            logger.warn("dispacth task for etl job " + processJobInstanceID + " of " + etlTask.filePath + " to " + etlTask.etlIpPort + " unsuccessfully");
//                        }
                    } else {
                        logger.warn("dispatch data collect job for data process job " + dataCollectJob.getProcessJobInstanceID() + " to " + host + " unsuccessfully for " + sl);
                    }
                    httppost.releaseConnection();
                } catch (Exception ex) {
                    logger.warn("dispatch data collect job for data process job " + dataCollectJob.getProcessJobInstanceID() + " unsuccessfully for " + ex.getMessage(), ex);
                } finally {
                    //end
                    if (succeeded) {
                        executingDataCollectJobSet.put(dataCollectJob.getProcessJobInstanceID(), dataCollectJob);
                        executingDataCollectJobSetLock.unlock();
                    } else {
                        dataCollectJobWaitingList.put(dataCollectJob);
                    }
                }
            } catch (Exception ex) {
                logger.warn("some error happened when doing data collect job dispatching for " + ex.getMessage(), ex);
            }
        }
    }
}
