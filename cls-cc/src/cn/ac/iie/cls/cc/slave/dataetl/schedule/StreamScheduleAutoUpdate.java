/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.dataetl.schedule;

import java.util.List;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
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
public class StreamScheduleAutoUpdate implements StreamScheduleInterface{
    //
    private static int UPDATE_INTERVAL_SECOND = -1;
    //log
    private static Logger logger = null;
    static {
	PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(BlockScheduleAutoUpdate.class.getName());
    }
    
    private  StreamAlgorithmInterface Algorithm;
    
    private final Scheduler scheduler;

    private StreamScheduleAutoUpdate() throws Exception {
	if(UPDATE_INTERVAL_SECOND <= 0 ){
	    UPDATE_INTERVAL_SECOND = ScheduleFactory.getUpdateIntervalSecond();
	}
	if(UPDATE_INTERVAL_SECOND <= 0){
	    throw new Exception("updateIntervalSecond illegal ! ("+UPDATE_INTERVAL_SECOND+")");
	}
	Algorithm = null;
	SchedulerFactory factory = new StdSchedulerFactory();
	scheduler = factory.getScheduler();
	JobDetail job = JobBuilder.newJob(AutoUpdateJob.class).withIdentity("AutoUpdateJob", "AutoUpdateGroup").build();
	Trigger trigger = TriggerBuilder.newTrigger().withIdentity("AutoUpdateTrigger", "AutoUpdateGroup").withSchedule(SimpleScheduleBuilder.simpleSchedule().repeatForever().withIntervalInSeconds(UPDATE_INTERVAL_SECOND)).startNow().build();
	scheduler.scheduleJob(job, trigger);
	scheduler.start();
    }
    
    public StreamScheduleAutoUpdate(ScheduleFactory.stdStreamAlgorithm algorithm) throws Exception {
	this();
	Algorithm = (StreamAlgorithmInterface) ScheduleFactory.stdStreamAlgorithmMap.get(algorithm).newInstance();
    }

    public StreamScheduleAutoUpdate(StreamAlgorithmInterface algorithm) throws Exception {
	this();
	Algorithm = algorithm;
    }
    
    @Override
    public synchronized TaskItem Schedule() {
	if(Algorithm == null){
	    logger.error("no algorithm was set!");
	    return null;
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
	try {
	    Thread.sleep(300);
	} catch (InterruptedException ex) {
	    ex.printStackTrace();
	}
	return Algorithm.Schedule( etlList);
    }

    @Override
    public StreamScheduleInterface setAlgorithm(StreamAlgorithmInterface algorithm) {
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
