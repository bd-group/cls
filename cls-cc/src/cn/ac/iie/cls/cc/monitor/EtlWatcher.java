/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.monitor;

import cn.ac.iie.cls.cc.commons.RuntimeEnv;
import cn.ac.iie.cls.cc.config.Configuration;
import cn.ac.iie.cls.cc.slave.dataetl.ETLJobTracker;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.StdSchedulerFactory;

/**
 *
 * @author L-R
 */
public class EtlWatcher {
    //数据根目录

    private static String CLSETL_ROOT;
    //
    private static List<String> etlServerIpPorts;
    //localport
    private static int LOCAL_PORT;
    //
    private static int TASK_TIMEOUT_SECOND;
    //
    private static int UPDATE_INTERVAL_SECOND;
    //
    private static final String TASK_TIMEOUT_JOB_GROUP = "TASK_TIMEOUT_JOB";
    //
    private static final String TASK_TIMEOUT_TRIGGER_GROUP = "TASK_TIMEOUT_TRIGGER";
    //
    private static Scheduler scheduler;
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(EtlWatcher.class.getName());
    }

    static public boolean StartWatching() throws Exception {
        String configurationFileName = "cls-cc.properties";

        logger.info("getting configuration from configuration file " + configurationFileName);
        Configuration conf = Configuration.getConfiguration(configurationFileName);
        if (conf == null) {
            throw new Exception("reading " + configurationFileName + " is failed.");
        }
        CLSETL_ROOT = conf.getString("clsETLRoot", "");
        if (CLSETL_ROOT.isEmpty()) {
            throw new Exception("definition clsETLRoot is not found in" + configurationFileName);
        }
        UPDATE_INTERVAL_SECOND = conf.getInt("updateIntervalSecond", -1);
        if (UPDATE_INTERVAL_SECOND == -1) {
            throw new Exception("definition updateIntervalSecond is not fond in" + configurationFileName);
        }

        logger.info("initializing cls cc EtlWatcher...");

        if (!RuntimeEnv.zk.exists(CLSETL_ROOT + "/slaves")) {
            RuntimeEnv.zk.createPersistent(CLSETL_ROOT + "/slaves");
        }

        etlServerIpPorts = RuntimeEnv.zk.getChildren(CLSETL_ROOT + "/slaves");
        RuntimeEnv.zk.subscribeChildChanges(CLSETL_ROOT + "/slaves", new IZkChildListener() {
            @Override
            public synchronized void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                boolean[] notCheckedLocal = new boolean[etlServerIpPorts.size()];
                int curNum = currentChilds.size();
                for (int i = 0; i < notCheckedLocal.length; ++i) {
                    notCheckedLocal[i] = true;
                }
                tag:
                for (int i = 0; i < curNum; ++i) {
                    for (int j = 0; j < notCheckedLocal.length; ++j) {
                        if (notCheckedLocal[j]) {
                            if (etlServerIpPorts.get(j).equals(currentChilds.get(i))) {
                                notCheckedLocal[j] = false;
                                continue tag;
                            }
                        }
                    }
                }
                List<String> downETLList = new ArrayList<String>();
                for (int i = 0; i < notCheckedLocal.length; ++i) {
                    if (notCheckedLocal[i]) {
                        downETLList.add(etlServerIpPorts.get(i));
                    }
                }
                //延时,给予etlServer 重新连接 zookeeper 的机会
                for (int i = 0; i < 1; ++i) {
                    Thread.sleep(UPDATE_INTERVAL_SECOND);
                    List<String> namesTmp = RuntimeEnv.zk.getChildren(CLSETL_ROOT + "/slaves");
                    for (String name : namesTmp) {
                        for (String curDownString : downETLList) {
                            if (name.equals(curDownString)) {
                                downETLList.remove(curDownString);
                            }
                        }
                        if (downETLList.isEmpty()) {
                            return;
                        }
                    }
                }
                etlServerIpPorts = currentChilds;
                if (downETLList.size() == 0) {
                    return;
                }
                ETLJobTracker.getETLJobTracker().rewaitByEtlIpPort(downETLList);
            }
        });
        logger.info("initialize cls cc EtlWatcher successfully");

        logger.info("initializing cls cc QuartzWatcher");
        TASK_TIMEOUT_SECOND = conf.getInt("taskTimeOutSecond", -1);
        if (TASK_TIMEOUT_SECOND == -1) {
            throw new Exception("definition taskTimeOutSecond is not found in" + configurationFileName);
        }
        SchedulerFactory factory = new StdSchedulerFactory();
        scheduler = factory.getScheduler();
        scheduler.start();

        logger.info("initialize cls cc QuartzWatcher successfully");
        return true;
    }

    static public boolean appendTaskWatcher(String taskId, String JobID) throws SchedulerException {
        JobDetail job = JobBuilder.newJob(TaskTimeOutJob.class).withIdentity(taskId, TASK_TIMEOUT_JOB_GROUP).withDescription(JobID).build();
        Trigger trigger = TriggerBuilder.newTrigger().withIdentity(taskId, TASK_TIMEOUT_TRIGGER_GROUP).withSchedule(SimpleScheduleBuilder.simpleSchedule().withRepeatCount(0)).startAt(new Date(System.currentTimeMillis() + 1000 * TASK_TIMEOUT_SECOND)).build();
        synchronized (scheduler) {
            scheduler.scheduleJob(job, trigger);
        }
        return true;
    }

    static public boolean checkJobExists(String taskId) {
        boolean existsTask = false;
        synchronized (scheduler) {
            try {
                existsTask = scheduler.checkExists(new TriggerKey(taskId, TASK_TIMEOUT_TRIGGER_GROUP));
            } catch (SchedulerException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            return existsTask;
        }
    }

    static public boolean deleteTaskWatcher(String taskId) throws SchedulerException {
        boolean rslt = true;
        synchronized (scheduler) {
            scheduler.pauseTrigger(new TriggerKey(taskId, TASK_TIMEOUT_TRIGGER_GROUP));
            //    if(!scheduler.unscheduleJob(new TriggerKey(filePath,TASK_TIMEOUT_TRIGGER_GROUP))){
            //	rslt = false;
            //   }
            if (!scheduler.deleteJob(new JobKey(taskId, TASK_TIMEOUT_JOB_GROUP))) {
                rslt = false;
            }
            String[] keys = scheduler.getContext().getKeys();
            for (String keyString : keys) {
                System.out.println(keyString);
            }
        }
        return rslt;
    }
}
