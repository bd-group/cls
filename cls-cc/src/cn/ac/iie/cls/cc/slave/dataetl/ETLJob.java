/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.dataetl;

import cn.ac.ict.ncic.util.dao.Dao;
import cn.ac.ict.ncic.util.dao.DaoPool;
import cn.ac.iie.cls.cc.commons.RuntimeEnv;
import cn.ac.iie.cls.cc.monitor.EtlWatcher;
import cn.ac.iie.cls.cc.slave.dataetl.schedule.ScheduleFactory;
import cn.ac.iie.cls.cc.slave.dataetl.schedule.StreamScheduleInterface;
import cn.ac.iie.cls.cc.slave.dataetl.schedule.TaskItem;
import cn.ac.iie.cls.cc.util.HttpResponseParser;
import cn.ac.iie.cls.cc.util.XMLReader;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.SQLException;
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

        QUEUING, RUNNING, SUCCEED, HALFSUCCEED, TERMINATED, ERROR
    }
    private JobStatus jobStatus;
    private Map<String, String> dataProcessDescriptor = new HashMap<String, String>();
    public static final String CLS_AGENT_DATA_COLLECT_DESC = "clsAgentETLDesc";
    public static final String DATA_ETL_DESC = "dataEtlDesc";
    private static final String PROCESS_JOB_DESC = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?><requestParams><processJobInstanceId>PROCESS_JOB_ID</processJobInstanceId><taskId>TASK_ID</taskId><redoTimes>REDO_TIMES</redoTimes><processConfig>PROCESS_CONFIG</processConfig></requestParams>";
    private int task2doNum;
    private BlockingQueue<ETLTask> etlTaskWaitingList = new LinkedBlockingQueue<ETLTask>();
    private Map<String, ETLTask> etlTaskSet = new HashMap<String, ETLTask>();
    private Lock etlTaskSetLock = new ReentrantLock();
    private Map<String, ETLTask> succeededETLTaskSet = new HashMap<String, ETLTask>();
    private Map<String, ETLTask> failedETLTaskSet = new HashMap<String, ETLTask>();
    private Map<String, Map> operatorStatSet = new HashMap<String, Map>();
    private Map<String, Set> operatorRelationSet = new HashMap<String, Set>();
    private final Map<String, Integer> taskDispatchTimes = new HashMap<String, Integer>();
    private final Map<String, Integer> taskFailedTimes = new HashMap<String, Integer>();
    private volatile boolean stop = false;
    private boolean jobSucceed = true;
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

    public static ETLJob getETLJob() {
        return new ETLJob();
    }

    public void setProcessJobInstanceID(String processJobInstanceID) {
        this.processJobInstanceID = processJobInstanceID;
    }

    public void setETLJobDescriptor(String pDescriptor) {
        dataProcessDescriptor.put(DATA_ETL_DESC, pDescriptor);
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
        Dao dao = DaoPool.getDao(RuntimeEnv.METADB_CLUSTER);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        String date = sdf.format(new Date());
        String sql = "update dp_job set task_num = " + pETLTaskList.size() + ", update_time = to_date('" + date + "', 'YYYY-MM-DD HH24:MI:SS') where job_id = '" + processJobInstanceID + "'";
        logger.info(sql);
        try {
            dao.executeUpdate(sql);
            for (ETLTask etlTask : pETLTaskList) {
                etlTaskWaitingList.put(etlTask);
                sql = "insert into dp_task values ('" + etlTask.taskId + "', '" + processJobInstanceID + "', '" + etlTask.filePath + "','" + ETLTask.ETLTaskStatus.ENQUEUE + "','" + etlTask.etlIpPort + "',''," + etlTask.dispatchTimes + ", " + etlTask.failedTimes + ", to_date('" + date + "', 'YYYY-MM-DD HH24:MI:SS'))";
                logger.info(sql);
                dao.executeUpdate(sql);
                taskDispatchTimes.put(etlTask.taskId, etlTask.dispatchTimes);
                taskFailedTimes.put(etlTask.taskId, etlTask.failedTimes);
                System.out.println("taskFailedTimes = ===  " + taskFailedTimes.get(etlTask.taskId));
                logger.info("task for etl job " + processJobInstanceID + " of " + etlTask.filePath + " has inserted into dp_task successfully");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }catch (Exception ex) {
            ex.printStackTrace();
        }

        Connection tmpConn = null;
        try {
            tmpConn = dao.getConnection();
            tmpConn.close();
        } catch (Exception ex) {
            logger.warn("errors exists in closing sql connection", ex);
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
        updateJobStatusInDB();

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

    public void updateJobStatusInDB() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Dao dao = DaoPool.getDao(RuntimeEnv.METADB_CLUSTER);
        String sql = "";
        String date = sdf.format(new Date());
        sql = "update dp_job set job_status = '" + jobStatus + "', update_time = to_date('" + date + "', 'YYYY-MM-DD HH24:MI:SS') where job_id = '" + this.processJobInstanceID + "'";
        logger.info(sql);
        try {
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
    }

    public void responseTask(List<ETLTask> pETLTaskList) {
        synchronized (this) {
            for (ETLTask etlTask : pETLTaskList) {
                Dao dao = DaoPool.getDao(RuntimeEnv.METADB_CLUSTER);
                String sql = "";
                switch (etlTask.taskStatus) {
                    case SUCCEEDED:
                        logger.info("task of " + etlTask.filePath + " for dataprocess job " + processJobInstanceID + " finished successfully");
                        try {
                            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                            String date = sdf.format(new Date());
                            synchronized (taskFailedTimes) {
                                etlTask.failedTimes = taskFailedTimes.get(etlTask.taskId);
                            }
                            sql = "update dp_task set task_status = '" + ETLTask.ETLTaskStatus.SUCCEEDED + "',failed_times = " + etlTask.failedTimes + ", dataprocess_stat = '" + etlTask.taskStat + "', update_time = to_date('" + date + "', 'YYYY-MM-DD HH24:MI:SS') "
                                    + "where job_id = '" + this.processJobInstanceID + "' and task_id = '" + etlTask.taskId + "' "
                                    + "and dispatch_times = " + etlTask.dispatchTimes + " and task_status = '" + ETLTask.ETLTaskStatus.PRECOMMIT + "'";
                            logger.info(sql);
                            //+ " and task_status <> " + ETLTask.ABORT + " and task_status <> " + ETLTask.CRASHED + " and task_status <> " + ETLTask.FAILED ;
                            System.out.println("job execute successful : time = " + date + " job_id = " + this.processJobInstanceID + "serverid = " + etlTask.etlIpPort);
                            dao.executeUpdate(sql);
                            EtlWatcher.deleteTaskWatcher(etlTask.taskId + "_" + etlTask.dispatchTimes);

                            if (taskDispatchTimes.get(etlTask.taskId) > 1) {
                                sql = "update dp_task set task_status = '" + ETLTask.ETLTaskStatus.ABORT + "' where job_id = '" + processJobInstanceID + "' and task_id = '" + etlTask.taskId + "' and dispatch_times <> " + etlTask.dispatchTimes;
                                logger.info(sql);
                                dao.executeUpdate(sql);
                            }
                        } catch (SchedulerException ex) {
                            java.util.logging.Logger.getLogger(ETLJob.class.getName()).log(Level.SEVERE, null, ex);
                        } catch (SQLException e) {
                            logger.error("sql exception : ", e);
                        } catch (Exception e) {
                            logger.error("there are errors in executing sql: " + sql, e);
                        }
                        succeededETLTaskSet.put(etlTask.taskId, etlTask);
                        taskDispatchTimes.remove(etlTask.taskId);

                        if (etlTask.taskStat == null || etlTask.taskStat.isEmpty()) {
                            logger.warn("it seems task task of " + etlTask.filePath + " for dataprocess job " + processJobInstanceID + " forgot to report statistic information");
                        } else {
                            updateJobStat(etlTask.taskStat);
                        }
                        etlTaskSet.remove(etlTask.taskId);
                        break;
                    case HALFSUCCEED:
                        logger.info("task of " + etlTask.filePath + " for dataprocess job " + processJobInstanceID + " finished halfsucceeded");
                        try {
                            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                            String date = sdf.format(new Date());
                            synchronized (taskFailedTimes) {
                                etlTask.failedTimes = taskFailedTimes.get(etlTask.taskId);
                            }
                            sql = "update dp_task set task_status = '" + ETLTask.ETLTaskStatus.HALFSUCCEED + "',failed_times = " + etlTask.failedTimes + ", dataprocess_stat = '" + etlTask.taskStat + "', update_time = to_date('" + date + "', 'YYYY-MM-DD HH24:MI:SS') "
                                    + "where job_id = '" + this.processJobInstanceID + "' and task_id = '" + etlTask.taskId + "' "
                                    + "and dispatch_times = " + etlTask.dispatchTimes + " and task_status = '" + ETLTask.ETLTaskStatus.PRECOMMIT + "'";
                            logger.info(sql);
                            System.out.println("job execute successful : time = " + date + " job_id = " + this.processJobInstanceID + "serverid = " + etlTask.etlIpPort);
                            dao.executeUpdate(sql);
                            EtlWatcher.deleteTaskWatcher(etlTask.taskId + "_" + etlTask.dispatchTimes);

                            if (taskDispatchTimes.get(etlTask.taskId) > 1) {
                                sql = "update dp_task set task_status = '" + ETLTask.ETLTaskStatus.ABORT + "' where job_id = '" + processJobInstanceID + "' and task_id = '" + etlTask.taskId + "' and dispatch_times <> " + etlTask.dispatchTimes;
                                logger.info(sql);
                                dao.executeUpdate(sql);
                            }
                        } catch (SchedulerException ex) {
                            java.util.logging.Logger.getLogger(ETLJob.class.getName()).log(Level.SEVERE, null, ex);
                        } catch (SQLException e) {
                            logger.error("sql exception : ", e);
                        } catch (Exception e) {
                            logger.error("there are errors in executing sql: " + sql, e);
                        }
                        succeededETLTaskSet.put(etlTask.taskId, etlTask);
                        taskDispatchTimes.remove(etlTask.taskId);

                        if (etlTask.taskStat == null || etlTask.taskStat.isEmpty()) {
                            logger.warn("it seems task task of " + etlTask.filePath + " for dataprocess job " + processJobInstanceID + " forgot to report statistic information");
                        } else {
                            updateJobStat(etlTask.taskStat);
                        }
                        etlTaskSet.remove(etlTask.taskId);
                        break;
                    case FAILED:
                        logger.warn("task of " + etlTask.filePath + " for dataprocess job " + processJobInstanceID + " finished unsuccessfully");
                        try {
                            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                            String date = sdf.format(new Date());
                            addFailedTime(etlTask.taskId);
                            synchronized (taskFailedTimes) {
                                etlTask.failedTimes = taskFailedTimes.get(etlTask.taskId);
                            }
                            sql = "update dp_task set task_status = '" + ETLTask.ETLTaskStatus.FAILED + "',failed_times = " + etlTask.failedTimes + " , dataprocess_stat = '" + etlTask.taskStat + "', update_time = to_date('" + date + "', 'YYYY-MM-DD HH24:MI:SS') "
                                    + "where job_id = '" + this.processJobInstanceID + "' and task_id = '" + etlTask.taskId + "' and dispatch_times = " + etlTask.dispatchTimes + " and (task_status = '" + ETLTask.ETLTaskStatus.EXECUTING + "' or task_status = '" + ETLTask.ETLTaskStatus.TIMEOUT + "')";
                            logger.info(sql);
                            logger.info("job execute failed : time = " + date + " job_id = " + this.processJobInstanceID + " task_file_path = " + etlTask.filePath + " task_id = " + etlTask.taskId);
                            dao.executeUpdate(sql);
                            
                            boolean existsInWaitingList = taskExistsInWaitingList(etlTask);
                            if (taskDispatchTimes.containsKey(etlTask.taskId)) {
                                EtlWatcher.deleteTaskWatcher(etlTask.taskId + "_" + etlTask.dispatchTimes);
                                if (taskDispatchTimes.get(etlTask.taskId) <= 3 && !existsInWaitingList) {
                                    System.out.println("错误重发，1.0版本直接重发" + taskDispatchTimes.get(etlTask.taskId));
                                    logger.info("this task is failed and it will be redispatched : time = " + date + " job_id = " + this.processJobInstanceID + " task_file_path = " + etlTask.filePath + " task_id = " + etlTask.taskId);
                                    ETLJobTracker.getETLJobTracker().rewaitByJobTask(processJobInstanceID, etlTask.taskId, true);
                                } else if (existsInWaitingList) {
                                    logger.info("another same etlTask is waiting to dispatch in etlWaitingList, so this task will not be dispatched");
                                } else {
                                    logger.info("this task failed and it has been redispathced for more then 2 times maybe it's an error task : time = " + date + " job_id = " + this.processJobInstanceID + " task_file_path = " + etlTask.filePath + " task_id = " + etlTask.taskId);

                                    if (etlTask.dispatchTimes >= 3) {
                                        logger.info("task failed in it's third dispatch, and it will be paused");
                                        pauseTask(etlTask.taskId, false);
                                    }

    //                                if (taskFailedTimes.containsKey(etlTask.taskId) && taskFailedTimes.get(etlTask.taskId) >= 3) {
    //                                    logger.info("task failed for 3 times, and it will be paused");
    //                                    pauseTask(etlTask.taskId, false);
    //                                }
                                    if (etlTask.taskStat == null || etlTask.taskStat.isEmpty()) {
                                        logger.warn("it seems task task of " + etlTask.filePath + " for dataprocess job " + processJobInstanceID + " forgot to report statistic information");
                                    } else {
                                        updateJobStat(etlTask.taskStat);
                                    }
                                }
                            } else { //该etlTask已经退出，etlJob还未退出此时etlServer返回FAILED
                                if (etlTask.taskStat == null || etlTask.taskStat.isEmpty()) {
                                    logger.warn("it seems task task of " + etlTask.filePath + " for dataprocess job " + processJobInstanceID + " forgot to report statistic information");
                                } else {
                                    updateJobStat(etlTask.taskStat);
                                }
                            }
                        } catch (SchedulerException ex) {
                            logger.warn("delete task watcher of task of " + etlTask.filePath + " for dataprocess job " + processJobInstanceID + " is failed for " + ex.getMessage(), ex);
                        } catch (SQLException e) {
                            logger.error("sql exception : ", e);
                        } catch (Exception e) {
                            logger.error("there are errors in executing sql: " + sql, e);
                        }
                        updateJobStat(etlTask.taskStat);
                        break;
                    case EXECUTING:
                        break;
                    default:
                        logger.warn("unknown task status " + etlTask.taskStatus + " for etl task of " + etlTask.filePath);
                }
                try {
                    Connection conn = dao.getConnection();
                    conn.close();
                } catch (Exception e) {
                    logger.warn("there errors in closing db connection", e);
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
        updateJobStatusInDB();
        while (true) {
            try {
                ETLTask etlTask = etlTaskWaitingList.peek();
                if (etlTask != null) {
                    boolean succeeded = false;
                    String host = "";
                    int port = 0;
                    //ScheduleFactory scft = new ScheduleFactory();
                    StreamScheduleInterface schd = ScheduleFactory.getStreamScheduleHandler(ScheduleFactory.UpdateMode.Auto, ScheduleFactory.stdStreamAlgorithm.Greedy);
                    TaskItem taskServer = schd.Schedule();
                    if (etlTask.dispatchTimes == 1) {
                        host = "http://" + taskServer.getEtlIp();
                        port = taskServer.getEtlPort();
                        etlTask.etlIpPort = taskServer.getEtlIp() + ":" + taskServer.getEtlPort();
                    } else {
                        System.out.println("3---" + ScheduleFactory.getEtlList().size());
                        if (ScheduleFactory.getEtlList() != null) {
                            if (ScheduleFactory.getEtlList().size() > 1) {
                                schd = ScheduleFactory.getStreamScheduleHandler(ScheduleFactory.UpdateMode.Auto, ScheduleFactory.stdStreamAlgorithm.Greedy);
                                ScheduleFactory.removeETLItem(etlTask.etlIpPort);
                                taskServer = schd.Schedule();
                                host = "http://" + taskServer.getEtlIp();
                                port = taskServer.getEtlPort();
                                etlTask.etlIpPort = taskServer.getEtlIp() + ":" + taskServer.getEtlPort();
                            } else {
                                host = "http://" + taskServer.getEtlIp();
                                port = taskServer.getEtlPort();
                                etlTask.etlIpPort = taskServer.getEtlIp() + ":" + taskServer.getEtlPort();
                            }
                        }
                    }

                    String content = dataProcessDescriptor.get(DATA_ETL_DESC).replace("#{FILE_PATH}", etlTask.filePath).replace("TASK_ID", etlTask.taskId).replace("REDO_TIMES", etlTask.dispatchTimes + "");
                    try {
                        System.out.println(host + ":" + port + "/resources/etltask/execute");
                        System.out.println(content);
                        HttpClient httpClient = new DefaultHttpClient();
                        HttpPost httppost = new HttpPost(host + ":" + port + "/resources/etltask/execute");
                        InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream(content.getBytes()), -1);
                        reqEntity.setContentType("binary/octet-stream");
                        reqEntity.setChunked(true);
                        httppost.setEntity(reqEntity);

                        //fixme:multi thread
                        etlTaskSetLock.lock();
                        HttpResponse response = httpClient.execute(httppost);
                        StatusLine sl = response.getStatusLine();
                        if (sl.getStatusCode() == 200) {
                            String rs = HttpResponseParser.getResponseContent(response);
                            System.out.println("response == = " + rs);
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
                        try {
                            Dao dao = DaoPool.getDao(RuntimeEnv.METADB_CLUSTER);
                            String sql = "";
                            if (succeeded) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                                String date = sdf.format(new Date());
                                sql = "update dp_task set task_status = '" + ETLTask.ETLTaskStatus.EXECUTING + "', etlserver_id = '" + etlTask.etlIpPort + "', update_time = to_date('" + date + "', 'YYYY-MM-DD HH24:MI:SS') where job_id = '"
                                        + this.processJobInstanceID + "' and task_id = '" + etlTask.taskId + "' and dispatch_times = " + etlTask.dispatchTimes + " and task_status = '" + ETLTask.ETLTaskStatus.ENQUEUE + "'";
                                logger.info(sql);
                                System.out.println("job despatched to etl dca_task : job_id = " + processJobInstanceID + " task_id = " + etlTask.taskId + " task_file_path = " + etlTask.filePath);
                                dao.executeUpdate(sql);
                                EtlWatcher.appendTaskWatcher(etlTask.taskId + "_" + etlTask.dispatchTimes, processJobInstanceID);
                                etlTaskWaitingList.take();
                                etlTaskSet.put(etlTask.taskId, etlTask);

                                synchronized (taskDispatchTimes) {
                                    if (taskDispatchTimes.get(etlTask.taskId) != null) {
                                        System.out.println("set redoTimes = " + taskDispatchTimes.get(etlTask.taskId) + " taskId = " + etlTask.taskId);
                                        taskDispatchTimes.put(etlTask.taskId, taskDispatchTimes.get(etlTask.taskId) + 1);
                                    }
                                }
                                logger.info("task for etl job " + processJobInstanceID + " of " + etlTask.filePath + " has removed from etlTaskWaitingList successfully");
                            } else {
                                logger.warn("dispatch task for etl job " + processJobInstanceID + " of " + etlTask.filePath + " to " + etlTask.etlIpPort + " unsuccessfully preparing to resend");
                            }

                            Connection tmpConn = null;
                            try {
                                tmpConn = dao.getConnection();
                                tmpConn.close();
                            } catch (Exception ex) {
                                logger.warn("errors exists in closing sql connection", ex);
                            }
                        } finally {
                            etlTaskSetLock.unlock();
                        }

                    }
                }

                synchronized (this) {
                    logger.info("etl job for data process job " + processJobInstanceID + " status--start at " + runStartTime + " td:" + task2doNum + ",tt:" + etlTaskSet.size() + ",st:" + succeededETLTaskSet.size() + ",ft:" + failedETLTaskSet.size());
                    if (task2doNum == 0) {
                        runEndTime = new Date();
                        jobStatus = JobStatus.SUCCEED;
                        logger.info("etl job for data process job " + processJobInstanceID + " is finished with no task to do at " + runEndTime);
                        break;
                    } else if (task2doNum > 0) {
                        int taskDoneNum = succeededETLTaskSet.size() + failedETLTaskSet.size();

                        if (taskDoneNum == task2doNum) {
                            runEndTime = new Date();
                            if (jobSucceed) {
                                jobStatus = JobStatus.SUCCEED;
                                logger.info("etl job for data process job " + processJobInstanceID + " is finished successfully at " + runEndTime + " with " + succeededETLTaskSet.size() + " succeeded task");
                            } else {
                                if (succeededETLTaskSet.isEmpty()) {
                                    jobStatus = JobStatus.ERROR;
                                    logger.error("etl job for data process job " + processJobInstanceID + " is finished partly succeed at " + runEndTime + " with " + failedETLTaskSet.size() + " failed task and " + succeededETLTaskSet.size() + " succeeded task");
                                } else {
                                    jobStatus = JobStatus.HALFSUCCEED;
                                    logger.info("etl job for data process job " + processJobInstanceID + " is finished successfully at " + runEndTime + " with " + succeededETLTaskSet.size() + " succeeded task");
                                }
                            }

                            ETLJobTracker.getETLJobTracker().removeJob(this);
                            if (!taskDispatchTimes.isEmpty()) {
                                Dao dao = DaoPool.getDao(RuntimeEnv.METADB_CLUSTER);
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                                String date = sdf.format(new Date());
                                try {
                                    Set<String> taskIdSet = taskDispatchTimes.keySet();
                                    Iterator<String> taskIdIt = taskIdSet.iterator();
                                    while (taskIdIt.hasNext()) {
                                        ETLTask abortTask = failedETLTaskSet.get(taskIdIt.next());
                                        System.out.println("task = " + abortTask);
                                        synchronized (taskFailedTimes) {
                                            System.out.println("taskid = " + abortTask.taskId);
                                            System.out.println("taskFailedTimes = ===  in : " + taskFailedTimes.get(abortTask.taskId));
                                            abortTask.failedTimes = taskFailedTimes.get(abortTask.taskId);
                                        }
                                        String sql = "insert into dp_task values ('" + abortTask.taskId + "', '" + processJobInstanceID + "', '" + abortTask.filePath + "','" + ETLTask.ETLTaskStatus.ABORT + "','" + abortTask.etlIpPort + "','" + abortTask.taskStat + "'," + abortTask.dispatchTimes + ", " + abortTask.failedTimes + ", to_date('" + date + "', 'YYYY-MM-DD HH24:MI:SS'))";
                                        logger.info(sql);
                                        dao.executeUpdate(sql);
                                    }
                                } catch (SQLException e) {
                                    e.printStackTrace();
                                } catch (Exception e) {
                                    e.printStackTrace();
                                } finally {
                                    Connection conn = null;
                                    try {
                                        conn = dao.getConnection();
                                        conn.close();
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    } finally {
                                        reportJobStat();
                                    }
                                }
                            } else {
                                reportJobStat();
                            }

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
        if (etlTaskSet.isEmpty()) {
            return false;
        }
        etlTaskSetLock.lock();
        try {
            for (Map.Entry<String, ETLTask> entry : etlTaskSet.entrySet()) {
                if (entry.getValue().etlIpPort.equals(EtlIpPort)) {
                    ETLTask rewaitTask = etlTaskSet.remove(entry.getKey());
                    if (rewaitTask == null) {
                        logger.info("can't rewaiting : fileString=" + entry.getKey() + ",etlIpPort=" + entry.getValue().etlIpPort);
                    } else {
                        EtlWatcher.deleteTaskWatcher(rewaitTask.taskId + "_" + rewaitTask.dispatchTimes);
                        etlTaskWaitingList.add(rewaitTask);
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

    public boolean rewaitByTaskId(String taskId, boolean falied) {
        etlTaskSetLock.lock();
        Dao dao = DaoPool.getDao(RuntimeEnv.METADB_CLUSTER);
        String sql = "";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        String date = sdf.format(new Date());

        ETLTask rewaitTask = etlTaskSet.remove(taskId);
        try {
            if (rewaitTask == null) {
                logger.info("can't rewaiting : fileString= " + taskId + ",jobId = " + processJobInstanceID);
            } else {
//                int redoTimes = 0;
                synchronized (taskDispatchTimes) {
                    if (taskDispatchTimes.containsKey(rewaitTask.taskId)) {
                        rewaitTask.dispatchTimes = taskDispatchTimes.get(rewaitTask.taskId);
                    }
                }
//                rewaitTask.dispatchTimes = redoTimes + 1;
                synchronized (taskFailedTimes) {
                    rewaitTask.failedTimes = taskFailedTimes.get(rewaitTask.taskId);
                }
                sql = "insert into dp_task values ('" + rewaitTask.taskId + "', '" + processJobInstanceID + "', '" + rewaitTask.filePath + "','" + ETLTask.ETLTaskStatus.ENQUEUE + "','" + rewaitTask.etlIpPort + "',''," + rewaitTask.dispatchTimes + ", " + rewaitTask.failedTimes + ", to_date('" + date + "', 'YYYY-MM-DD HH24:MI:SS'))";
                logger.info(sql);
                try {
                    dao.executeUpdate(sql);
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

                logger.info("task for etl job " + processJobInstanceID + " of " + rewaitTask.filePath + " has inserted into dp_task successfully");
                etlTaskWaitingList.add(rewaitTask);
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            Connection tmpConn = null;
            try {
                tmpConn = dao.getConnection();
                tmpConn.close();
            } catch (Exception ex) {
                logger.warn("errors exists in closing sql connection", ex);
            }
            etlTaskSetLock.unlock();
        }

        return false;
    }

    public void dataCollectJobInform() {
        stop = true;
    }

    public static int showTaskStatus(String jobId, String taskId, String ipPort) {
        HttpClient httpClient = new DefaultHttpClient();
        HttpPost httppost = new HttpPost("http://" + ipPort + "/resources/etltask/status");
        InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream((jobId + "_" + taskId).getBytes()), -1);

        reqEntity.setContentType("binary/octet-stream");
        reqEntity.setChunked(true);
        httppost.setEntity(reqEntity);
        System.out.println(httppost.getURI() + " from askWhetherETLJobCompleted");
        int taskStatus = 0;
        try {
            HttpResponse response = httpClient.execute(httppost);
            StatusLine sl = response.getStatusLine();
            if (sl.getStatusCode() == 200) {      //200 404
                String result = HttpResponseParser.getResponseContent(response);
                System.out.println("hhttphttpresult ====== == = = = = = " + result);
                if (result.trim().equals("true")) {
                    taskStatus = 1;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            httppost.releaseConnection();
        }
        System.out.println("taskStatus ====== == = = = = = " + taskStatus);
        return taskStatus;
    }

    public ETLTask getETLTask(String taskId) {
        return etlTaskSet.get(taskId);
    }

    public boolean taskExists(String taskId) {
        return taskDispatchTimes.containsKey(taskId);
    }

    public int getTaskDispatchTimes(String taskId) {
        synchronized (taskDispatchTimes) {
            if (taskDispatchTimes.containsKey(taskId)) {
                return taskDispatchTimes.get(taskId);
            }
        }
        return 3;
    }

    public void setTaskDispatchTimes(String taskId, int times) {
        synchronized (taskDispatchTimes) {
            if (taskDispatchTimes.containsKey(taskId)) {
                taskDispatchTimes.put(taskId, times);
            }
        }
    }

    public void appendFailedTask(ETLTask etlTask) {
        failedETLTaskSet.put(etlTask.taskId, etlTask);
    }

    public void appendSucceedTask(ETLTask etlTask) {
        succeededETLTaskSet.put(etlTask.taskId, etlTask);
    }

    public boolean addFailedTime(String taskId) {
        synchronized (taskFailedTimes) {
            if (taskFailedTimes.containsKey(taskId)) {
                taskFailedTimes.put(taskId, taskFailedTimes.get(taskId) + 1);
                return true;
            } else {
                taskFailedTimes.put(taskId, taskFailedTimes.get(taskId) + 1);
                return true;
            }
        }
    }

    public void setFailedTime(String taskId, int times) {
        synchronized (taskFailedTimes) {
            taskFailedTimes.put(taskId, times);
        }
    }

    public int getFailedTime(String taskId) {
        synchronized (taskFailedTimes) {
            if (taskFailedTimes.containsKey(taskId)) {
                return taskFailedTimes.get(taskId);
            }
        }
        return -1;
    }

    public int getTask2doNum() {
        return task2doNum;
    }

    public void setJobSucceedFalse() {
        jobSucceed = false;
    }
    
    public boolean taskExistsInWaitingList(ETLTask etlTask) {
        return etlTaskWaitingList.contains(etlTask);
    }

    public boolean pauseTask(String taskId, boolean timeOutTask) {
        Dao dao = DaoPool.getDao(RuntimeEnv.METADB_CLUSTER);
        String sql = "";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        String date = sdf.format(new Date());

        ETLTask rewaitTask = etlTaskSet.remove(taskId);
        try {
            if (rewaitTask == null) {
                logger.info("no task in the etlTaskSet to pause : taskId = " + taskId + ",jobId = " + processJobInstanceID);
            } else {
                jobSucceed = false;
                failedETLTaskSet.put(rewaitTask.taskId, rewaitTask);
                synchronized (taskFailedTimes) {
                    rewaitTask.failedTimes = taskFailedTimes.get(rewaitTask.taskId);
                }
                if (timeOutTask) {
                    sql = "update dp_task set task_status = '" + ETLTask.ETLTaskStatus.TIMEOUT + "', failed_times = " + rewaitTask.failedTimes + ", update_time = to_date('" + date + "', 'YYYY-MM-DD HH24:MI:SS') where job_id = '" + processJobInstanceID + "' and task_id = '" + rewaitTask.taskId + "' and dispatch_times = '" + rewaitTask.dispatchTimes + "' and task_status = '" + ETLTask.ETLTaskStatus.EXECUTING + "'";
                } else {
                    taskDispatchTimes.remove(taskId);
                    sql = "insert into dp_task values ('" + rewaitTask.taskId + "', '" + processJobInstanceID + "', '" + rewaitTask.filePath + "','" + ETLTask.ETLTaskStatus.ERRORTASK + "','" + rewaitTask.etlIpPort + "','" + rewaitTask.taskStat + "', " + rewaitTask.dispatchTimes + ", " + rewaitTask.failedTimes + ", to_date('" + date + "', 'YYYY-MM-DD HH24:MI:SS'))";
                }
                logger.info(sql);
                try {
                    dao.executeUpdate(sql);
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                //fix me 调用etl的pause函数
                logger.info("task for etl job " + processJobInstanceID + " of " + rewaitTask.filePath + " has been paused successfully");
                return true;
            }

            if (timeOutTask) {
                //fix me 调用etl的stop
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            Connection tmpConn = null;
            try {
                tmpConn = dao.getConnection();
                tmpConn.close();
            } catch (Exception ex) {
                logger.warn("errors exists in closing sql connection", ex);
            }
        }

        return false;
    }
}
