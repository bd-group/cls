/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.cc.slave.status;

import cn.ac.iie.cls.etl.dataprocess.config.Configuration;
import java.net.InetAddress;
import java.util.Date;
import java.util.concurrent.locks.Lock;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.quartz.DateBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

/**
 *
 * @author L-R
 */
public class StatusUpdate {
    //超时时间
    private static int SESSION_TIMEOUT ;
    //主机地址
    private static String ZK_CLUSTER;
    //数据根目录
    public static String CLSCC_ROOT;
    public static String CLSETL_ROOT;
    //etl信息存储位置
    public static String ETL_IPPORT ;
    //
    private static int UPDATE_INTEVAL;
    
    static public ZkClient zk = null;
    
    static private Logger logger = null;
    
    static private boolean isUpdating = false;
    static private Lock updateLock;
    static private Lock zkLock;
    
    static {
		PropertyConfigurator.configure("log4j.properties");
	    logger = Logger.getLogger(StatusUpdate.class.getName());
    }
    
    static public synchronized boolean startAutoUpdate() throws Exception{
		String configurationFileName = "cls-etl.properties";
		logger.info("initializing cls etl zkclient...");
		logger.info("getting configuration from configuration file " + configurationFileName);
		Configuration conf = Configuration.getConfiguration(configurationFileName);
		if (conf == null) {
		    throw new Exception("reading " + configurationFileName + " is failed.");
		}
		ZK_CLUSTER = conf.getString("zkCluster", "");
		if(ZK_CLUSTER.isEmpty()){
		    throw new Exception("definition zkCluster is not fond in"+configurationFileName);
		}
		SESSION_TIMEOUT = conf.getInt("zkSessionTimeout", -1);
		if(SESSION_TIMEOUT == -1){
		    throw new Exception("definition zkSessionTimeout is not fond in"+configurationFileName);
		}
		CLSCC_ROOT = conf.getString("clsCCRoot", "");
		if(CLSCC_ROOT.isEmpty()){
		    throw new Exception("definition clsCCRoot is not fond in"+configurationFileName);
		}
		CLSETL_ROOT = conf.getString("clsETLRoot", "");
		if(CLSETL_ROOT.isEmpty()){
		    throw new Exception("definition clsETLRoot is not fond in"+configurationFileName);
		}
		int localPort = conf.getInt("jettyServerPort", -1);
		if(localPort == -1 ){
		    throw new Exception("definition jettyServerPort is not fond in"+configurationFileName);
		}
		UPDATE_INTEVAL = conf.getInt("updateInterval", -1);
		if(UPDATE_INTEVAL == -1){
		    throw new Exception("definition zkRegPath is not fond in"+configurationFileName);
		}
		ETL_IPPORT = InetAddress.getLocalHost().getHostAddress().toString() + ":"+String.valueOf(localPort);
		logger.info("get configuration from configuration file ");
		
		logger.info("initializing cls etl zkclient ...");
		zk  =  new ZkClient(ZK_CLUSTER, SESSION_TIMEOUT,SESSION_TIMEOUT+1);
		if(!zk.exists(CLSETL_ROOT)){
		    zk.createPersistent(CLSETL_ROOT);
		}
		
		if (!zk.exists(CLSETL_ROOT+"/slaves")) {
			zk.createPersistent(CLSETL_ROOT+"/slaves");
		}
		
		if (!zk.exists(CLSETL_ROOT+"/slaves/"+ETL_IPPORT)) {
			zk.createEphemeral(CLSETL_ROOT+"/slaves/"+ETL_IPPORT, ETL_IPPORT+"|0|"+ System.currentTimeMillis()+"|1|0");
		}
		if (!zk.exists(CLSETL_ROOT+"/task2recover")) {
			zk.createPersistent(CLSETL_ROOT+"/task2recover");
		}
		
		logger.info("initialize cls etl zkclient successfully");
		
		logger.info("starting status auto update ...");
		UpdateJob.init(zk,CLSETL_ROOT+"/slaves/"+ETL_IPPORT,ETL_IPPORT);
		SchedulerFactory factory = new StdSchedulerFactory();
		Scheduler scheduler = factory.getScheduler();
		Date runTime = DateBuilder.nextGivenSecondDate(null, UPDATE_INTEVAL);
		JobDetail job = JobBuilder.newJob(UpdateJob.class).withIdentity("autoUpdateStatusJob", "autoUpdateGroup").build();
		Trigger trigger = TriggerBuilder.newTrigger().withIdentity("autoUpdateStatusTrigger", "autoUpdateGroup").withSchedule(SimpleScheduleBuilder.simpleSchedule().repeatForever().withIntervalInSeconds(UPDATE_INTEVAL)).startAt(runTime).build();
		scheduler.scheduleJob(job, trigger);
		scheduler.start();
		logger.info("start status auto update successfully");
		return true;
    }
    static public boolean newTaskDown(){
    	return UpdateJob.newTaskDown();
    }
    static public boolean newTaskDown(int taskNum){
        return UpdateJob.newTaskDown(taskNum);
    }
}
