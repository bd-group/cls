/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.dataetl;

import cn.ac.iie.cls.cc.commons.RuntimeEnv;
import cn.ac.iie.cls.cc.monitor.EtlWatcher;
import cn.ac.iie.cls.cc.slave.dataetl.schedule.ScheduleFactory;
import cn.ac.iie.cls.cc.slave.dataetl.schedule.StreamScheduleInterface;
import cn.ac.iie.cls.cc.slave.dataetl.schedule.TaskItem;
import cn.ac.iie.cls.cc.util.HttpResponseParser;
import cn.ac.iie.cls.cc.util.XMLReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.XMLWriter;
import org.quartz.SchedulerException;

/**
 *
 * @author alexmu
 */
public class ETLJob implements Runnable {

    private String processJobInstanceID = "";
    private Date dispatchedTime = null;
    private Date runStartTime = null;
    private Date runEndTime = null;
    private boolean needReport = true;

    public enum JobStatus {

        QUEUING, RUNNING, SUCCEED, TERMINATED, ERROR
    }
    private JobStatus jobStatus;
    private Map<String, String> dataProcessDescriptor = new HashMap<String, String>();
    public static final String CLS_AGENT_DATA_COLLECT_DESC = "clsAgentETLDesc";
    public static final String DATA_ETL_DESC = "dataEtlDesc";
    private static final String PROCESS_JOB_DESC = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?><requestParams><processJobInstanceId>PROCESS_JOB_ID</processJobInstanceId><processConfig>PROCESS_CONFIG</processConfig></requestParams>";
    private int task2doNum;
    private BlockingQueue<ETLTask> etlTaskWaitingList = new LinkedBlockingQueue<ETLTask>();
    private Map<String, ETLTask> etlTaskRuningSet = new HashMap<String, ETLTask>();
    private Lock etlTaskSetLock = new ReentrantLock();
    private Map<String, ETLTask> succeededETLTaskSet = new HashMap<String, ETLTask>();
    private Map<String, ETLTask> failedETLTaskSet = new HashMap<String, ETLTask>();
    private Map<String, Map> operatorStatSet = new HashMap<String, Map>();
    private Map<String, Set> operatorRelationSet = new HashMap<String, Set>();
    private volatile boolean stop = false;
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(ETLJob.class.getName());
    }

    private ETLJob(boolean pNeedReport) {
        this();
        needReport = pNeedReport;
    }

    private ETLJob() {
        dispatchedTime = new Date();
    }

    public static ETLJob getETLJob(String pProcessJobDescriptor, boolean pNeedReport) {
        logger.debug(pProcessJobDescriptor);
        ETLJob dataProcessJob = new ETLJob(pNeedReport);
        try {
            Document processJobDoc = DocumentHelper.parseText(pProcessJobDescriptor);
            Element processJobInstanceElt = processJobDoc.getRootElement();

            Element processJobInstanceIdElt = processJobInstanceElt.element("processJobInstanceId");
            if (processJobInstanceIdElt == null) {
                logger.error("no processJobInstanceId element found in " + pProcessJobDescriptor);
                dataProcessJob = null;
            } else {
                dataProcessJob.processJobInstanceID = processJobInstanceIdElt.getStringValue();
                Element processConfigElt = processJobInstanceElt.element("processConfig");
                if (processConfigElt == null) {
                    logger.error("no processConfig element found in " + pProcessJobDescriptor);
                    dataProcessJob = null;
                } else {
                    dataProcessJob.parse(processConfigElt.element("operator").asXML());
                }
            }
            dataProcessJob.task2doNum = -1;
        } catch (Exception ex) {
            logger.error("creating data process job instance is failed for " + ex.getMessage(), ex);
            dataProcessJob = null;
        }
        return dataProcessJob;
    }

    private String getXmlString(Element pOperatorNode) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        XMLWriter xw = new XMLWriter(bos);
        xw.write(pOperatorNode);
        xw.close();
        xw = null;
        return new String(bos.toByteArray(), "UTF-8");
    }

    private void parse(String pDataProcessDescriptor) throws Exception {
        logger.debug(pDataProcessDescriptor);
        Document dataProcessDocument = DocumentHelper.parseText(pDataProcessDescriptor);
        Element operatorNode = dataProcessDocument.getRootElement();

        String operatorNodeXmlStr = getXmlString(operatorNode);
        logger.debug("data process descriptor:" + operatorNodeXmlStr);

        List<Element> elements = operatorNode.elements("operator");

        int clsAgentETLOperatorDescCnt = 0;
        String clsAgentETLOperatorName = "";
        for (Element element : elements) {
            if (element.attributeValue("class").startsWith("GetherDataFrom")) {
                dataProcessDescriptor.put(CLS_AGENT_DATA_COLLECT_DESC, PROCESS_JOB_DESC.replace("PROCESS_JOB_ID", processJobInstanceID).replace("PROCESS_CONFIG", getXmlString(element)));
                clsAgentETLOperatorName = element.attributeValue("name");
                operatorNode.remove(element);
                clsAgentETLOperatorDescCnt++;
            }
        }


        if (clsAgentETLOperatorDescCnt > 0) {
            elements = operatorNode.elements("connect");
            int clsAgentETLConnectCnt = 0;
            for (Element element : elements) {
                if (element.attributeValue("from").startsWith(clsAgentETLOperatorName)) {
                    operatorNode.remove(element);
                    clsAgentETLConnectCnt++;
                }
            }
        }

        elements = operatorNode.elements("connect");
        for (Element element : elements) {
            if (element.attributeValue("from").startsWith("parent.in")) {
                operatorNode.remove(element);
            }
            if (element.attributeValue("to").startsWith("parent.out")) {
                operatorNode.remove(element);
            }
        }


        dataProcessDescriptor.put(DATA_ETL_DESC, PROCESS_JOB_DESC.replace("PROCESS_JOB_ID", processJobInstanceID).replace("PROCESS_CONFIG", getXmlString(operatorNode)));
    }

    public String getProcessJobInstanceID() {
        return processJobInstanceID;
    }

    public Map<String, String> getDataProcessDescriptor() {
        return dataProcessDescriptor;
    }

    public JobStatus getJobStatus() {
        return jobStatus;
    }

    public void setJobStatus(JobStatus jobStatus) {
        this.jobStatus = jobStatus;
    }

    public String getInputFilePathStr() {
        String filePath = "";
        try {
            Document dataProcessDocument = DocumentHelper.parseText(dataProcessDescriptor.get(DATA_ETL_DESC));
            Element operatorNode = dataProcessDocument.getRootElement();

            String operatorNodeXmlStr = getXmlString(operatorNode);
            logger.debug("data process descriptor:" + operatorNodeXmlStr);

            List<Element> elements = operatorNode.element("processConfig").element("operator").elements("operator");

            for (Element element : elements) {
                if (element.attributeValue("class").startsWith("TXTFileInput") || element.attributeValue("class").startsWith("CSVFileInput") || element.attributeValue("class").startsWith("XMLFileInput")) {
                    List<Element> paramElts = element.elements("parameter");
                    for (Element paramElt : paramElts) {
                        if (paramElt.attributeValue("name").endsWith("File")) {
                            filePath = paramElt.getTextTrim();
                        }
                    }
                }
            }
            return filePath;
        } catch (Exception ex) {
            return null;
        }
    }

    public synchronized void appendTask(List<ETLTask> pETLTaskList) {
        for (ETLTask etlTask : pETLTaskList) {
            try {
                etlTaskWaitingList.put(etlTask);
            } catch (Exception ex) {
            }
        }
    }

    private synchronized void updateJobStat(String pTaskStat) {
        String[] operatorStatItems = pTaskStat.split("\\$");
        for (String operatorStatItem : operatorStatItems) {
            String[] statItems = operatorStatItem.split(":");
            String[] operatorDescItems = statItems[0].split(",");
            Set<String> subOperatorSet = operatorRelationSet.get(operatorDescItems[1]);
            if (subOperatorSet == null) {
                subOperatorSet = new TreeSet<String>();
                subOperatorSet.add(operatorDescItems[0]);
                operatorRelationSet.put(operatorDescItems[1], subOperatorSet);
            } else {
                subOperatorSet.add(operatorDescItems[0]);
            }

            if (statItems.length > 1) {
                String[] portMetricItems = statItems[1].split(",");
                Map<String, Integer> portMetricSet = operatorStatSet.get(operatorDescItems[0]);
                if (portMetricSet == null) {
                    portMetricSet = new HashMap<String, Integer>();
                    operatorStatSet.put(operatorDescItems[0], portMetricSet);
                }
                for (String portMetricItem : portMetricItems) {
                    String[] kvs = portMetricItem.split("\\-");
                    Integer metric = portMetricSet.get(kvs[0]);
                    if (metric == null) {
                        portMetricSet.put(kvs[0], new Integer(Integer.parseInt(kvs[1])));
                    } else {
                        int metricn = metric.intValue() + Integer.parseInt(kvs[1]);
                        portMetricSet.put(kvs[0], new Integer(metricn));
                    }
                }
            }

        }
    }

    private String getOperatorStat(String pOperatorName, String pParentOperatorUri) {
        String operatorStat = "<operateTracker><trackerId>TRACKER_ID</trackerId><jobInstanceId>JOB_INSTANCE_ID</jobInstanceId><operatorUri>OPERATOR_URI</operatorUri><duration></duration><runningState>SUCCEED</runningState><errorMessage></errorMessage><portCounters>PORT_COUNTERS</portCounters><children>CHILDREN</children></operateTracker>";
        String portCounterTemplate = "<portCounter><portName>PORT_NAME</portName><rowSetCount>0</rowSetCount><rowCount>ROW_COUNT</rowCount></portCounter>";

        operatorStat = operatorStat.replace("TRACKER_ID", processJobInstanceID + "_" + pOperatorName);
        operatorStat = operatorStat.replace("JOB_INSTANCE_ID", processJobInstanceID);
        operatorStat = operatorStat.replace("OPERATOR_URI", pParentOperatorUri + "/" + pOperatorName);

        Map<String, Integer> portMetrics = operatorStatSet.get(pOperatorName);
        String portCounters = "";
        if (portMetrics != null) {
            Iterator portMetricEntryItor = portMetrics.entrySet().iterator();
            String portCounter = "";
            while (portMetricEntryItor.hasNext()) {
                Entry portMetricEntry = (Entry) portMetricEntryItor.next();
                portCounter = portCounterTemplate.replace("PORT_NAME", (String) portMetricEntry.getKey());
                portCounter = portCounter.replace("ROW_COUNT", ((Integer) portMetricEntry.getValue()).toString());
                portCounters += portCounter;
            }
            operatorStat = operatorStat.replace("PORT_COUNTERS", portCounters);
        } else {
            operatorStat = operatorStat.replace("PORT_COUNTERS", "");
        }

        Set subOperatorSet = operatorRelationSet.get(pOperatorName);
        if (subOperatorSet != null) {
            String subOperatorStat = "";
            Iterator subOPeratorItor = subOperatorSet.iterator();
            while (subOPeratorItor.hasNext()) {
                String subOperatorName = (String) subOPeratorItor.next();
                subOperatorStat += getOperatorStat(subOperatorName, pParentOperatorUri + "/" + pOperatorName);
            }
            operatorStat = operatorStat.replace("CHILDREN", subOperatorStat);
        } else {
            operatorStat = operatorStat.replace("CHILDREN", "");
        }

        return operatorStat;
    }

    private void reportJobStat() {
        if (!needReport) {
            return;
        }
        String stat = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?><requestParams><priority>PRIORITY</priority><dispatchedTime>DISPATCHED_TIME</dispatchedTime><runStartTime>RUN_START_TIME</runStartTime><runEndTime>RUN_END_TIME</runEndTime><state>STATE</state><errorMessage></errorMessage>#STAT</requestParams>";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        stat = stat.replace("PRIORITY", "PROCESS_PRIORITY_NORMAL");
        stat = stat.replace("DISPATCHED_TIME", sdf.format(dispatchedTime));
        stat = stat.replace("RUN_START_TIME", sdf.format(runStartTime));
        stat = stat.replace("RUN_END_TIME", sdf.format(runEndTime));
        switch (jobStatus) {
            case SUCCEED:
                stat = stat.replace("STATE", "SUCCEED");
                break;
            case ERROR:
                stat = stat.replace("STATE", "ERROR");
                break;
            case TERMINATED:
                stat = stat.replace("STATE", "TERMINATED");
                break;
            default:
                stat = stat.replace("STATE", "UNKNOWN");
        }

        Set topOperatorSet = operatorRelationSet.get("null");
        if (topOperatorSet == null) {
            stat = stat.replace("#STAT", "");
        } else {
            String topOperator = (String) (operatorRelationSet.get("null").iterator().next());
            stat = stat.replace("#STAT", getOperatorStat(topOperator, ""));
        }

        logger.info(stat);

        try {
            HttpClient httpClient = new DefaultHttpClient();
            logger.info(RuntimeEnv.getParam(RuntimeEnv.SYSTEM_CC_ROOT_URI) + "/CoLinkage/resources/processJobInstance/" + processJobInstanceID + "/result");
            HttpPost httppost = new HttpPost(RuntimeEnv.getParam(RuntimeEnv.SYSTEM_CC_ROOT_URI) + "/CoLinkage/resources/processJobInstance/" + processJobInstanceID + "/result");

            InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream(stat.getBytes()), -1);
            //reqEntity.setContentType("binary/octet-stream");
            reqEntity.setContentType("application/xml");//; charset=UTF-8
//            reqEntity.setChunked(true);
            httppost.setEntity(reqEntity);
            HttpResponse response = httpClient.execute(httppost);
            //fixme            
            logger.info("reponse to system cc " + RuntimeEnv.getParam(RuntimeEnv.SYSTEM_CC_ROOT_URI) + ":" + response.getStatusLine());
            //fixme:dispose task dispatch error                        
            httppost.releaseConnection();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void responseTask(List<ETLTask> pETLTaskList) {
        synchronized (this) {
            for (ETLTask etlTask : pETLTaskList) {
                switch (etlTask.taskStatus) {
                    case ETLTask.SUCCEEDED:
                        logger.info("task of " + etlTask.filePath + " for dataprocess job " + processJobInstanceID + " finished successfully");
                        try {
                            EtlWatcher.deleteTaskWatcher(etlTask.filePath);
                        } catch (SchedulerException ex) {
                            java.util.logging.Logger.getLogger(ETLJob.class.getName()).log(Level.SEVERE, null, ex);
                        }
                        succeededETLTaskSet.put(etlTask.filePath, etlTask);

                        if (etlTask.taskStat == null || etlTask.taskStat.isEmpty()) {
                            logger.warn("it seems task task of " + etlTask.filePath + " for dataprocess job " + processJobInstanceID + " forgot to report statistic information");
                        } else {
                            updateJobStat(etlTask.taskStat);
                        }
                        etlTaskRuningSet.remove(etlTask.filePath);
                        break;
                    case ETLTask.FAILED:
                        logger.warn("task of " + etlTask.filePath + " for dataprocess job " + processJobInstanceID + " finished unsuccessfully");
                        try {
                            EtlWatcher.deleteTaskWatcher(etlTask.filePath);
                        } catch (SchedulerException ex) {
                            logger.warn("delete task watcher of task of " + etlTask.filePath + " for dataprocess job " + processJobInstanceID + " is failed for " + ex.getMessage(), ex);
                        }
                        failedETLTaskSet.put(etlTask.filePath, etlTask);
//                        updateJobStat(etlTask.taskStat);
                        etlTaskRuningSet.remove(etlTask.filePath);
                        break;
                    case ETLTask.EXECUTING:
                        break;
                    default:
                        logger.warn("unknown task status " + etlTask.taskStatus + " for etl task of " + etlTask.filePath);
                }
            }
        }
    }

    public void setTask2doNum(int task2doNum) {
        this.task2doNum = task2doNum;
    }

    @Override
    public void run() {
        runStartTime = new Date();
        jobStatus = JobStatus.RUNNING;
        while (true) {
            try {
                ETLTask etlTask = etlTaskWaitingList.peek();
                if (etlTask != null) {
                    boolean succeeded = false;
                    StreamScheduleInterface schd = ScheduleFactory.getStreamScheduleHandler(ScheduleFactory.UpdateMode.Auto, ScheduleFactory.stdStreamAlgorithm.Greedy);
                    TaskItem task = schd.Schedule();
                    String host = "http://" + task.getEtlIp();
                    int port = task.getEtlPort();
                    etlTask.etlIpPort = task.getEtlIp() + ":" + task.getEtlPort();

                    String content = dataProcessDescriptor.get(DATA_ETL_DESC).replace("#{FILE_PATH}", etlTask.filePath);
                    try {
                        HttpClient httpClient = new DefaultHttpClient();
                        HttpPost httppost = new HttpPost(host + ":" + port + "/resources/etltask/execute");

                        InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream(content.getBytes()), -1);
                        reqEntity.setContentType("binary/octet-stream");
                        reqEntity.setChunked(true);
                        httppost.setEntity(reqEntity);
                        etlTaskSetLock.lock();
                        //fixme:multi thread
                        HttpResponse response = httpClient.execute(httppost);
                        StatusLine sl = response.getStatusLine();
                        if (sl.getStatusCode() == 200) {
                            String rs = HttpResponseParser.getResponseContent(response);
                            if (rs.trim().equals("ok")) {
                                logger.info("dispacth task for etl job " + processJobInstanceID + " of " + etlTask.filePath + " to " + etlTask.etlIpPort + " successfully");
                                succeeded = true;
                            } else {
                                logger.warn("dispacth task for etl job " + processJobInstanceID + " of " + etlTask.filePath + " to " + etlTask.etlIpPort + " unsuccessfully");
                            }
                        } else {
                            logger.warn("dispacth task for etl job " + processJobInstanceID + " of " + etlTask.filePath + " to " + etlTask.etlIpPort + " unsuccessfully");
                        }
                        //fixme:dispose task dispatch error  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!                      
                        httppost.releaseConnection();
                    } catch (Exception ex) {
                        logger.warn("dispatch task for etl job " + processJobInstanceID + " of " + etlTask.filePath + " to " + etlTask.etlIpPort + " unsuccessfully for " + ex.getMessage(), ex);
                    } finally {
                        //end
                        if (succeeded) {
                            EtlWatcher.appendTaskWatcher(etlTask.filePath, processJobInstanceID);
                            etlTaskWaitingList.take();
                            etlTaskRuningSet.put(etlTask.filePath, etlTask);
                            logger.info("task for etl job " + processJobInstanceID + " of " + etlTask.filePath + " has removed from etlTaskWaitingList successfully");
                        }
                        logger.info("etl job for data process job " + processJobInstanceID + " etlTaskWaitingList size:" + etlTaskWaitingList.size());
                        etlTaskSetLock.unlock();
                    }
                }

                synchronized (this) {
                    logger.info("etl job for data process job " + processJobInstanceID + " status--start at " + runStartTime + " td:" + task2doNum + ",tt:" + etlTaskRuningSet.size() + ",st:" + succeededETLTaskSet.size() + ",ft:" + failedETLTaskSet.size());
                    if (task2doNum == 0) {
                        runEndTime = new Date();
                        jobStatus = JobStatus.SUCCEED;
                        logger.info("etl job for data process job " + processJobInstanceID + " is finished with no task to do at " + runEndTime);
                        reportJobStat();
                        break;
                    } else if (task2doNum > 0) {
                        int taskDoneNum = succeededETLTaskSet.size() + failedETLTaskSet.size();

                        if (taskDoneNum == task2doNum) {
                            runEndTime = new Date();
                            if (succeededETLTaskSet.size() == 0) {
                                jobStatus = JobStatus.ERROR;
                                logger.error("etl job for data process job " + processJobInstanceID + " is finished unsuccessfully at " + runEndTime + " with " + failedETLTaskSet.size() + " failed task");
                            } else if (failedETLTaskSet.size() == 0) {
                                jobStatus = JobStatus.SUCCEED;
                                logger.info("etl job for data process job " + processJobInstanceID + " is finished successfully at " + runEndTime + " with " + succeededETLTaskSet.size() + " succeeded task");
                            } else {
                                //fixme
                                jobStatus = JobStatus.SUCCEED;
                                logger.warn("etl job for data process job " + processJobInstanceID + " is finished partially successfully at " + runEndTime + " with " + succeededETLTaskSet.size() + " succeeded task and " + failedETLTaskSet.size() + " failed task");
                            }
                            reportJobStat();
                            ETLJobTracker.getETLJobTracker().removeJob(this);
                            break;
                        }
                    }
                }

                Thread.sleep(1000);
            } catch (Exception ex) {
                ex.printStackTrace();
                logger.warn("some error happened when doing etl task tracking of etl job for data process job " + processJobInstanceID + " for " + ex.getMessage(), ex);
            }
        }

//        this.notifyAll();

    }

    private void sendHttpRequest() {
    }

    public static void main(String[] args) {
        String dataProcessDescriptor = XMLReader.getXMLContent("trx-f_917mt-dp-spec.xml");
        System.out.println(dataProcessDescriptor);
        ETLJob dataProcessJob = ETLJob.getETLJob(dataProcessDescriptor, false);
        System.out.println(dataProcessJob.getDataProcessDescriptor().get(ETLJob.DATA_ETL_DESC));
        System.out.println("parse ok");
    }

    public boolean rewaitByEtlIpPort(String EtlIpPort) {
        if (etlTaskRuningSet.isEmpty()) {
            return false;
        }
        etlTaskSetLock.lock();
        try {
            for (Map.Entry<String, ETLTask> entry : etlTaskRuningSet.entrySet()) {
                if (entry.getValue().etlIpPort.equals(EtlIpPort)) {
                    ETLTask rewaitTask = etlTaskRuningSet.remove(entry.getKey());
                    if (rewaitTask == null) {
                        logger.info("can't rewaiting : fileString=" + entry.getKey() + ",etlIpPort=" + entry.getValue().etlIpPort);
                    } else {
                        EtlWatcher.deleteTaskWatcher(rewaitTask.filePath);
                        etlTaskWaitingList.add(rewaitTask);
                        logger.info("data process task for data process job " + processJobInstanceID + " of " + rewaitTask.filePath + " is timeout and requeue");
                    }
                }
            }
        } catch (SchedulerException ex) {
            java.util.logging.Logger.getLogger(ETLJob.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            etlTaskSetLock.unlock();
        }
        return true;
    }

    public boolean rewaitByFilePath(String filePath) {
        etlTaskSetLock.lock();
        try {
            ETLTask rewaitTask = etlTaskRuningSet.remove(filePath);
            if (rewaitTask == null) {
                logger.info("can't rewaiting : fileString= " + filePath + ",jobId = " + processJobInstanceID);
            } else {
                etlTaskWaitingList.add(rewaitTask);
            }
        } finally {
            etlTaskSetLock.unlock();
        }
        return true;
    }

    public void dataCollectJobInform() {
        stop = true;
    }
}
