/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.monitor;

import cn.ac.ict.ncic.util.dao.Dao;
import cn.ac.ict.ncic.util.dao.DaoPool;
import cn.ac.iie.cls.cc.commons.RuntimeEnv;
import cn.ac.iie.cls.cc.slave.dataetl.ETLJob;
import cn.ac.iie.cls.cc.slave.dataetl.ETLJobTracker;
import cn.ac.iie.cls.cc.slave.dataetl.ETLTask;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.List;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerException;

/**
 *
 * @author L-R
 */
public class TaskTimeOutJob implements Job {

    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(ETLJob.class.getName());
    }
    
    @Override
    public void execute(JobExecutionContext jec) throws JobExecutionException {
        try {
            JobDetail job = jec.getJobDetail();
            String jobId = job.getDescription();
            String jobKey = jec.getJobDetail().getKey().getName();
            String taskId = jobKey.substring(0, jobKey.length()-2);
            System.out.println("task timeout job ================== " + jec.getJobDetail().getDescription() + ", " + jec.getJobDetail().getKey().getName());
            jec.getScheduler().pauseTrigger(jec.getTrigger().getKey());
            //    jec.getScheduler().unscheduleJob(jec.getTrigger().getKey());
            jec.getScheduler().deleteJob(jec.getJobDetail().getKey());
            boolean taskDone = false;
            //查询zk中是否有当前条目
            List<String> childs = RuntimeEnv.zk.getChildren(RuntimeEnv.CLSETL_DATAROOT + "/task2recover");

            if (childs.size() > 0) {
                for (int i = 0; i < childs.size(); i++) {
                    String response = RuntimeEnv.zk.readData(RuntimeEnv.CLSETL_DATAROOT + "/task2recover/" + childs.get(i)).toString();
                    //task exit | executeResult | jobId | taskId | filePath | dataProcessStat | etlIpport | needresend(boolean) | redoNum(int)
                    String[] contents = response.split("[|]");
                    String responseJobId = contents[2];
                    String responseTaskId = contents[3];
                    if (responseJobId.equals(jobId) && responseTaskId.equals(taskId)) {
                        taskDone = true;
                    }
                }
            }

            if (!taskDone) {
                //向etl发询问消息
                ETLJobTracker ejt = ETLJobTracker.getETLJobTracker();
                ETLJob etlJob = ejt.getJob(jobId);
                ETLTask etlTask = null;
                if (etlJob != null) {
                    System.out.println("1---" + taskId + ", " + etlJob.getETLTask(taskId));
                    etlTask = etlJob.getETLTask(taskId);

                    if (etlTask != null) {
                        //1.0版本直接重发
                        int redoTimes = etlJob.getTaskDispatchTimes(taskId);
                        if (redoTimes < 3) {
                            System.out.println("超时重发，1.0版本直接重发 " + redoTimes);
                            
                            String sql = "";
                            Dao dao = DaoPool.getDao(RuntimeEnv.METADB_CLUSTER);

                            try {
                                sql = "update dp_task set task_status = '" + ETLTask.ETLTaskStatus.TIMEOUT + "' where job_id = '" + jobId + "' and task_id = '" + taskId + "' and task_status = '" + ETLTask.ETLTaskStatus.EXECUTING + "'"; 
                                dao.executeUpdate(sql);
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
                                }
                            }
                            
                            logger.info("this task is tasktimeout, and it is redispatched to etl : taskId = " + taskId + " redoTimes = " + redoTimes);
                            ejt.rewaitByJobTask(jobId, taskId, false);
                        } else {
                            logger.info("task run time out for 3 times, and it will be paused");
                            etlJob.appendFailedTask(etlTask);
                        }
                    }
                }
            }
        } catch (SchedulerException ex) {
            ex.printStackTrace();
        }
    }
}
