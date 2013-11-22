/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.cc.slave.etltask;

import cn.ac.iie.cls.etl.cc.slave.status.StatusUpdate;
import cn.ac.iie.cls.etl.cc.util.HttpResponseParser;
import cn.ac.iie.cls.etl.dataprocess.DataProcessFactory;
import cn.ac.iie.cls.etl.dataprocess.operator.DataProcess;
import cn.ac.iie.cls.etl.dataprocess.operator.Operator;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

/**
 *
 * @author alexmu
 */
public class ETLTask implements Runnable {

    private String processJobInstanceID = "";
    private String filePath = "";
    private String taskId = "";
    private int operatorNum = 0;
    private int redoTimes = 0;
    DataProcess dataProcess = null;
    Map<Operator, Map<String, Long>> operatorReportStore = new HashMap<Operator, Map<String, Long>>();
    Map<Operator, Thread> operatorStore = new HashMap<Operator, Thread>();
    static Logger logger = null;
    static BufferedWriter bw;
    static private int aaa = 0;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(ETLTask.class.getName());
    }

    private ETLTask() {
    }

    public static ETLTask getETLTask(String pETLProcessDescriptor) {
        ETLTask etlTask = new ETLTask();
        try {
            Document processJobDoc = DocumentHelper.parseText(pETLProcessDescriptor);
            Element processJobInstanceElt = processJobDoc.getRootElement();

            Element processJobInstanceIdElt = processJobInstanceElt.element("processJobInstanceId");
            Element taskIdElt = processJobInstanceElt.element("taskId");
            Element redoNumElt = processJobInstanceElt.element("redoTimes");
            if (processJobInstanceIdElt == null || taskIdElt == null || redoNumElt == null) {
                logger.error("no processJobInstanceId or taskId element found in " + pETLProcessDescriptor);
                etlTask = null;
            } else {
                etlTask.processJobInstanceID = processJobInstanceIdElt.getStringValue();
                etlTask.taskId = taskIdElt.getStringValue();
                etlTask.redoTimes = Integer.parseInt(redoNumElt.getStringValue());
                Element processConfigElt = processJobInstanceElt.element("processConfig");
                if (processConfigElt == null) {
                    logger.error("no processConfig element found in " + pETLProcessDescriptor);
                    etlTask = null;
                } else {
                    List<Element> operatorElts = processConfigElt.element("operator").elements("operator");

                    for (Element operatorElt : operatorElts) {
                        if (operatorElt.attributeValue("class").endsWith("FileInput")) {
                            List<Element> paramElts = operatorElt.elements("parameter");
                            for (Element paramElt : paramElts) {
                                if (paramElt.attributeValue("name").endsWith("File")) {
                                    etlTask.filePath = paramElt.getStringValue();
                                }
                            }
                            break;
                        }
                    }

                    logger.debug(processConfigElt.element("operator").asXML());
                    DataProcess dataProcess = DataProcessFactory.getDataProcess(processConfigElt.element("operator"));
                    if (dataProcess == null) {
                        logger.error("parse failed");
                        Thread.sleep(10000);
                        etlTask = null;
                    } else {
                        etlTask.dataProcess = dataProcess;
                        etlTask.dataProcess.setTaskManager(etlTask);
                    }
                }
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            etlTask = null;
        }
        return etlTask;
    }

    public String getProcessJobInstanceID() {
        return processJobInstanceID;
    }

    public String getID() {
        return processJobInstanceID + "_" + taskId;
    }

    public void run() {
        Thread t = new Thread(dataProcess);
        t.start();
        String dataProcessStat = "";
        while (true) {
            try {
                //report port metrics of operator
                synchronized (operatorReportStore) {
                    Iterator operatorIter = operatorReportStore.entrySet().iterator();
                    dataProcessStat = "";
                    while (operatorIter.hasNext()) {
                        Map.Entry operatorEntry = (Map.Entry<String, String>) operatorIter.next();
                        Operator operator = (Operator) operatorEntry.getKey();
                        Operator parentOperator = operator.getParentOperator();
                        if (dataProcessStat.isEmpty()) {
                            if (parentOperator == null) {
                                dataProcessStat = operator.getName() + ",null" + ":";
                            } else {
                                dataProcessStat = operator.getName() + "," + parentOperator.getName() + ":";
                            }
                        } else {
                            dataProcessStat += "$";
                            if (parentOperator == null) {
                                dataProcessStat += operator.getName() + ",null" + ":";
                            } else {
                                dataProcessStat += operator.getName() + "," + parentOperator.getName() + ":";
                            }
                        }
                        Iterator portIter = ((Map) operatorEntry.getValue()).entrySet().iterator();
                        String operatorStat = "";
                        while (portIter.hasNext()) {
                            Map.Entry portEntry = (Map.Entry<String, String>) portIter.next();
                            if (operatorStat.isEmpty()) {
                                operatorStat += portEntry.getKey() + "-" + portEntry.getValue();
                            } else {
                                operatorStat += "," + portEntry.getKey() + "-" + portEntry.getValue();
                            }
                        }
                        dataProcessStat += operatorStat;
                        logger.info(dataProcessStat);
                    }
                }
                System.out.println("dataProcess.isDone() = = " + dataProcess.isDone());
                if (dataProcess.isDone()) {
                    break;
                }
                Thread.sleep(3000);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        System.out.println("zero info task exited");
        String taskStatus = this.getTaskStatus();
        taskStatus = "FAILED";

        for (int i = 0; i < 200; i++) {
            System.out.println("finally status = " + taskStatus);
        }
        ETLTaskTracker.getETLTaskTracker().removeTask(this);
        String lockPath = StatusUpdate.CLSCC_ROOT + "/master/lock";
        String dataPath = StatusUpdate.CLSETL_ROOT + "/task2recover/task";

        if (StatusUpdate.zk.exists(lockPath)) {
            String ccIpPort = StatusUpdate.zk.readData(lockPath);
            boolean commit = false;
            boolean taskFailed = false;
            ////先查看zk中有没有需要恢复的数据
            //HALFSUCCEEDED/SUCCEEDED/FAILED/FINISHED
            System.out.println("first info = " + taskStatus);
            if (taskStatus.equals("HALFSUCCEEDED") || taskStatus.equals("SUCCEEDED")) {
                //预提交，并询问cc是否可以存储到HDFS中 
                String content = "task_precommit|" + processJobInstanceID + "|" + taskId + "|" + redoTimes;
                commit = askWhetherCommit(ccIpPort, content);
                System.out.println("second info = " + content + ", result = " + commit);
            } else {
                commit = true;
                taskFailed = true;
            }

            if (commit) {
                if (!taskFailed) {
                    commit();
                }
                HttpClient httpClient = new DefaultHttpClient();
                HttpPost httppost = new HttpPost("http://" + ccIpPort + "/resources/dataetl/etltaskreport");

                InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream(("task_exit|" + taskStatus + "|" + processJobInstanceID + "|" + taskId + "|" + filePath + "|" + dataProcessStat + "|" + StatusUpdate.ETL_IPPORT + "|" + false + "|" + redoTimes).getBytes()), -1);
                System.out.println("third info = " + "task_exit|" + taskStatus + "|" + processJobInstanceID + "|" + taskId + "|" + filePath + "|" + dataProcessStat + "|" + StatusUpdate.ETL_IPPORT + "|" + false + "|" + redoTimes);
                reqEntity.setContentType("binary/octet-stream");
                reqEntity.setChunked(true);
                httppost.setEntity(reqEntity);
                try {
                    httpClient.execute(httppost);
                } catch (Exception ex) {
                    logger.warn("can't connect to current master, execute result stored into zookeeper, current master might be crashed");
                    StatusUpdate.zk.createPersistentSequential(dataPath, "task_exit|" + taskStatus + "|" + processJobInstanceID + "|" + taskId + "|" + filePath + "|" + dataProcessStat + "|" + StatusUpdate.ETL_IPPORT + "|" + true + "|" + redoTimes);
                    System.out.println("dataPath === = = == = =" + dataPath);
                }

                /*List<String> childs = StatusUpdate.zk.getChildren(dataPath);
	    		
                 int childSize = childs.size();
                 if (childSize > 0) {
                 for (int i=0; i<childSize; i++) {
                 String finishedTask = StatusUpdate.zk.readData(childs.get(i));
                 String[] info = finishedTask.split("[|]");
                 if (info[6].equals("true") && info[5].equals(StatusUpdate.ETL_IPPORT)) {
                 InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream(finishedTask.getBytes()), -1);
                 reqEntity.setContentType("binary/octet-stream");
                 reqEntity.setChunked(true);
                 httppost.setEntity(reqEntity);
                 try {
                 HttpResponse response = httpClient.execute(httppost);
                 StatusLine sl = response.getStatusLine();
                 if(sl.getStatusCode()==200){      //200 404
                 String result = HttpResponseParser.getResponseContent(response);
                 System.out.println("hhttphttpresult ====== == = = = = = " + result);
                 if(result.trim().equals("true")){
                 startTask();
                 }
                 }
                 StatusUpdate.zk.delete(childs.get(i));
                 } catch (Exception ex) {
                 logger.info("can't connect to current master, current master might be crashed");
                 }
                 }
                 }
                 }*/

                httppost.releaseConnection();
            }
        } else {
            //fixme
            //StatusUpdate.zk.createPersistentSequential(dataPath, "task_exit|" + taskStatus + "|" + processJobInstanceID + "|" + taskId + "|" + filePath + "|" + dataProcessStat + "|" + StatusUpdate.ETL_IPPORT + "|" + false + "|" + redoTimes);
            logger.info("cc lock not exists, execute result stored into zookeeper");
        }

        logger.info("task for " + filePath + " data process job " + processJobInstanceID + " exits");
    }

    public static boolean askWhetherCommit(String ipport, String content) {
        HttpClient httpClient = new DefaultHttpClient();
        HttpPost httppost = new HttpPost("http://" + ipport + "/resources/dataetl/etltaskreport");
        InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream((content).getBytes()), -1);

        reqEntity.setContentType("binary/octet-stream");
        reqEntity.setChunked(true);
        httppost.setEntity(reqEntity);
        try {
            HttpResponse response = httpClient.execute(httppost);
            StatusLine sl = response.getStatusLine();
            if (sl.getStatusCode() == 200) {      //200 404
                String result = HttpResponseParser.getResponseContent(response);
                System.out.println("hhttphttpresult ====== == = = = = = " + result);
                if (result.trim().equals("true")) {
                    return true;
                }
            }
        } catch (Exception ex) {
            logger.info("can't connect to current master, execute result stored into zookeeper, current master might be crashed");
        }
        httppost.releaseConnection();
        return false;
    }

    public void report(Operator pOperator, Map<String, Long> portMetrics) {
        synchronized (operatorReportStore) {
            operatorReportStore.put(pOperator, portMetrics);
        }
    }

    /**
     * get task execute status
     *
     * @author IcerHan
     * @return task status
     */
    public String getTaskStatus() {
        //EXECUTING/HALFSUCCEEDED/SUCCEEDED/FAILED/FINISHED
        //return "FAILED";
        if (dataProcess.isDone()) {
//	    	Set<Operator> operatorSet = operatorReportStore.keySet();
//	    	Iterator<Operator> operatorIter = operatorSet.iterator();
            Set<Operator> opertorSet = operatorStore.keySet();
            Iterator<Operator> operatorIter = opertorSet.iterator();
            boolean hasSucceeded = false;
            boolean hasFailed = false;
            while (operatorIter.hasNext()) {
                Operator oper = operatorIter.next();
                System.out.println("operator status : " + oper.getName() + ", " + oper.getStatus());
                if (oper.getStatus() == Operator.SUCCEEDED) {
                    hasSucceeded = true;
                }
                if (oper.getStatus() == Operator.FAILED) {
                    hasFailed = true;
                }
                if (hasSucceeded && hasFailed) {
                    return "HALFSUCCEEDED";
                }
            }

            if (hasSucceeded && !hasFailed) {
                return "SUCCEEDED";
            }
            if (hasFailed && !hasSucceeded) {
                return "FAILED";
            }
        }
        return "EXECUTING";
    }

    public void putOperatorStore(Operator pOperator, Thread pThread) {
        operatorStore.put(pOperator, pThread);
    }

    public void stopTask() {
        //fix me
        Set<Operator> operatorSet = operatorStore.keySet();
        Iterator<Operator> operatorItor = operatorSet.iterator();
        try {
            while (operatorItor.hasNext()) {
                Operator op = operatorItor.next();
                synchronized (op) {
                    op.wait();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void startTask() {
        //fix me
        Set<Operator> operatorSet = operatorStore.keySet();
        Iterator<Operator> operatorItor = operatorSet.iterator();
        try {
            while (operatorItor.hasNext()) {
                Operator op = operatorItor.next();
                op.start();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void commit() {
        //fix me
        Set<Operator> operatorSet = operatorStore.keySet();
        Iterator<Operator> operatorItor = operatorSet.iterator();
        try {
            while (operatorItor.hasNext()) {
                Operator op = operatorItor.next();
                synchronized (op) {
                    op.commit();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
