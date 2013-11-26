/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.dataetl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author alexmu
 */
public class ETLJobTracker implements Runnable {

    private static BlockingQueue<ETLJob> etlJobWaitingList = new LinkedBlockingQueue<ETLJob>();
    private static Map<String, ETLJob> etlJobSet = new HashMap<String, ETLJob>();
    private Lock etlJobSetLock = new ReentrantLock();
    private static ETLJobTracker etlJobTracker = null;
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(ETLJobTracker.class.getName());
    }

    private ETLJobTracker() {
    }

    public static synchronized ETLJobTracker getETLJobTracker() {
        if (etlJobTracker == null) {
            etlJobTracker = new ETLJobTracker();
            Thread etlJobTrackerRunner = new Thread(etlJobTracker);
            etlJobTrackerRunner.start();
        }
        return etlJobTracker;
    }

    public void appendJob(ETLJob pETLJob) {
        try {
            etlJobWaitingList.put(pETLJob);
            pETLJob.setJobStatus(ETLJob.JobStatus.QUEUING);
        } catch (Exception ex) {
        }
    }

    public void removeJob(ETLJob pETLJob) {
        try {
            etlJobSetLock.lock();
            etlJobSet.remove(pETLJob.getProcessJobInstanceID());
        } finally {
            etlJobSetLock.unlock();
        }
    }

    public ETLJob getJob(String pDataProcessInstanceId) {
        ETLJob etlJob = null;
        try {
            etlJobSetLock.lock();
            etlJob = etlJobSet.get(pDataProcessInstanceId);
        } finally {
            etlJobSetLock.unlock();
        }
        return etlJob;
    }

    public void appendTask(String pDataProcessInstanceId, List<ETLTask> pETLTaskList) {
        ETLJob etlJob = null;
        try {
            etlJobSetLock.lock();
            etlJob = etlJobSet.get(pDataProcessInstanceId);
        } finally {
            etlJobSetLock.unlock();
        }
        if (etlJob == null) {
            logger.warn("can't find job with id:" + pDataProcessInstanceId);
            return;
        }
        etlJob.appendTask(pETLTaskList);

    }

    public void responseTask(String pDataProcessInstanceId, List<ETLTask> pETLTaskList) {
        ETLJob etlJob = null;
        try {
            etlJobSetLock.lock();
            etlJob = etlJobSet.get(pDataProcessInstanceId);
        } finally {
            etlJobSetLock.unlock();
        }
        if (etlJob == null) {
            logger.warn("can't find job with id:" + pDataProcessInstanceId);
            return;
        }
        etlJob.responseTask(pETLTaskList);

    }

    @Override
    public void run() {
        ETLJob etlJob = null;
        while (true) {
            try {
                etlJob = etlJobWaitingList.take();
                Thread etlJobRunner = new Thread(etlJob);
                etlJobRunner.start();
                etlJobSetLock.lock();
                etlJobSet.put(etlJob.getProcessJobInstanceID(), etlJob);
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                etlJobSetLock.unlock();
            }
        }
    }

    public boolean rewaitByEtlIpPort(String EtlIpPort) {
        etlJobSetLock.lock();
        try {
            if (etlJobSet.isEmpty()) {
                return false;
            }
            for (Map.Entry<String, ETLJob> entry : etlJobSet.entrySet()) {
                entry.getValue().rewaitByEtlIpPort(EtlIpPort);
            }
        } finally {
            etlJobSetLock.unlock();
        }
        return true;
    }

    public boolean rewaitByEtlIpPort(List<String> EtlIpPortList) {
        boolean rslt = true;
        for (String etlIpPort : EtlIpPortList) {
            if (rewaitByEtlIpPort(etlIpPort) == false) {
                rslt = false;
            }
        }
        return rslt;
    }

    public boolean rewaitByJobTask(String JobId, String taskId, boolean failed) {
        if (null == getJob(JobId)) {
            System.out.println("can't find job with id:" + JobId);
            return false;
        } else {
            return getJob(JobId).rewaitByTaskId(taskId, failed);
        }
    }
}
