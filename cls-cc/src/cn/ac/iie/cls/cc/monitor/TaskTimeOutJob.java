/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.monitor;

import cn.ac.iie.cls.cc.slave.dataetl.ETLJobTracker;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerException;

/**
 *
 * @author L-R
 */
public class TaskTimeOutJob implements Job{

    @Override
    public void execute(JobExecutionContext jec) throws JobExecutionException {
	try {
	    JobDetail job = jec.getJobDetail();
	    String jobId = job.getDescription();
	    jec.getScheduler().pauseTrigger(jec.getTrigger().getKey());
	//    jec.getScheduler().unscheduleJob(jec.getTrigger().getKey());
	    jec.getScheduler().deleteJob(jec.getJobDetail().getKey());
	    ///向etl发询问消息
	    ETLJobTracker.getETLJobTracker().rewaitByJobTask(jobId,job.getKey().getName());
	} catch (SchedulerException ex) {
	    ex.printStackTrace();
	}
    }
    
}
