/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.dataetl.schedule;

import cn.ac.iie.cls.cc.config.Configuration;
import static cn.ac.iie.cls.cc.slave.dataetl.schedule.ScheduleOld.logger;
import java.util.List;
import java.util.logging.Level;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.ScheduleBuilder;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

/**
 *
 * @author L_R
 */
public class BlockScheduleAutoUpdate implements BlockScheduleInterface{
    //
    private static int UPDATE_INTERVAL_SECOND;
    //log
    private static Logger logger = null;
    static {
	PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(BlockScheduleAutoUpdate.class.getName());
    }
    
    private  BlockAlgorithmInterface Algorithm;
    
    private final Scheduler scheduler;

    private BlockScheduleAutoUpdate() throws SchedulerException, Exception{
	Algorithm = null;
	SchedulerFactory factory = new StdSchedulerFactory();
	scheduler = factory.getScheduler();
	JobDetail job = JobBuilder.newJob(AutoUpdateJob.class).build();
	Trigger trigger = TriggerBuilder.newTrigger().withSchedule(SimpleScheduleBuilder.simpleSchedule().repeatForever().withIntervalInSeconds(UPDATE_INTERVAL_SECOND)).startNow().build();
	scheduler.scheduleJob(job, trigger);
	scheduler.start();
    }
    
    public BlockScheduleAutoUpdate(ScheduleFactory.stdBlockAlgorithm algorithm) throws InstantiationException, IllegalAccessException, Exception {
	this();
	Algorithm = (BlockAlgorithmInterface) ScheduleFactory.stdBlockAlgorithmMap.get(algorithm).newInstance();
    }

    public BlockScheduleAutoUpdate(BlockAlgorithmInterface algrithm) throws SchedulerException, Exception {
	this();
	Algorithm = algrithm;
    }
      
    @Override
    public synchronized boolean Schedule(List<TaskItem> taskList) {
	if(Algorithm == null){
	    logger.error("no algorithm was set!");
	    return false;
	}
	List<EtlItem>etlList;
	while((etlList = ScheduleFactory.getEtlList() ) == null){
	    try {
		logger.info("etlServerList not fond , retry in "+UPDATE_INTERVAL_SECOND+" seconds");
		Thread.sleep(UPDATE_INTERVAL_SECOND*1000);
	    } catch (InterruptedException ex) {
		logger.warn(ex.getMessage() , ex.getCause());
	    }
	}
	return Algorithm.Schedule(taskList, etlList);
    }

    @Override
    public BlockScheduleInterface setAlgorithm(BlockAlgorithmInterface algorithm) {
	Algorithm = algorithm;
	return this;
    }

    @Override
    public void Destruct() {
	try {
	    scheduler.pauseAll();
	    scheduler.clear();
	    scheduler.shutdown();
	} catch (SchedulerException ex) {
	    logger.warn(ex.getMessage(),ex.getCause());
	}
    }
    
}
