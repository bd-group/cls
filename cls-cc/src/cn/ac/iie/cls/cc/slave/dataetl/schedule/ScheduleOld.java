/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.dataetl.schedule;

import cn.ac.iie.cls.cc.config.Configuration;
import java.io.IOException;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.zookeeper.KeeperException;


/**
 *
 * @author L-R
 */
public class ScheduleOld {
    //超时时间
    private static  int SESSION_TIMEOUT;
    //主机地址
    private static  String CONNECT_STRING;
//    //CC要求数据时改变的位置
 //   private static  String CHECK_PATH;
    //数据根目录
    private static  String DATA_ROOT;
    //localport
    private static int LOCAL_PORT;
    //Zookeeper 的实例
    ZkClient zk;
    List<TaskItem>taskList;
    
    static private ScheduleOld schdl = null;
    
    static Logger logger = null;
    
    static {
	PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(ScheduleOld.class.getName());
    }
    
    
    private ScheduleOld() throws IOException, KeeperException, InterruptedException, Exception{
	String configurationFileName = "cls-cc.properties";
	logger.info("initializing cls cc zkclient...");
	logger.info("getting configuration from configuration file " + configurationFileName);
	Configuration conf = Configuration.getConfiguration(configurationFileName);
        if (conf == null) {
            throw new Exception("reading " + configurationFileName + " is failed.");
        }
	CONNECT_STRING = conf.getString("zkConnectString", "");
	if(CONNECT_STRING.isEmpty()){
	    throw new Exception("definition zkConnectString is not fond in"+configurationFileName);
	}
	SESSION_TIMEOUT = conf.getInt("zkSessionTimeout", -1);
	if(SESSION_TIMEOUT == -1){
	    throw new Exception("definition zkSessionTimeout is not fond in"+configurationFileName);
	}
	DATA_ROOT = conf.getString("zkDataRoot", "");
	if(DATA_ROOT.isEmpty()){
	    throw new Exception("definition zkDataRoot is not fond in"+configurationFileName);
	}
	LOCAL_PORT = conf.getInt("zkLocalPort", -1);
	if(LOCAL_PORT == -1){
	    throw new Exception("definition zkLocalPort is not fond in"+configurationFileName);
	}
	
	zk = new ZkClient(CONNECT_STRING, SESSION_TIMEOUT);
	taskList = new ArrayList<TaskItem>();
	if(!zk.exists(DATA_ROOT)){
	    zk.createPersistent(DATA_ROOT, InetAddress.getLocalHost().getHostAddress().toString()+":"+LOCAL_PORT);
	}
	logger.info("initialize cls cc zkclient successfully");
    }
    public boolean getBestSchedule(List<TaskItem> taskLs) throws KeeperException, InterruptedException, Exception{
	taskList = taskLs;
	
	if(!zk.exists(DATA_ROOT) ){
	    throw new Exception("not find Data_Root ?! 我了个去!");
	}
	List<String> etlNodeList = zk.getChildren(DATA_ROOT);
	int i = 0 ;
	while(etlNodeList.size() == 0 ){
	    System.out.println(new SimpleDateFormat("yy-MM-dd hh:mm:ss").format(new Date())+"\tNo etlServer exist,retry in "+ (1+6*(i>10?10:i))+"second");
	    Thread.sleep(1000+6000*(i>10?10:i));
	    etlNodeList = zk.getChildren(DATA_ROOT);
	    i++;
	}
	List<EtlItem> etlList = new ArrayList<EtlItem>();
	for( String item:etlNodeList){
	    String zkStr =zk.readData(DATA_ROOT+"/"+item);
	    etlList.add(Transform.Trans(zkStr));
	}
	ScheduleInterfaceOld schdlI = ScheduleFactoryOld.getScedule(taskList, etlList);
	if(schdlI.Schedule()){
	    return true;
	}
	return false;
    }
    public synchronized boolean getBestSchedule(TaskItem taskItem) throws Exception{
	if(!zk.exists(DATA_ROOT) ){
	    throw new Exception("can't find Data_Root !");
	}
	List<String> etlNodeList = zk.getChildren(DATA_ROOT);
	int i = 0 ;
	while(etlNodeList.size() == 0 ){
	    System.out.println(new SimpleDateFormat("yy-MM-dd hh:mm:ss").format(new Date())+"\tNo etlServer exist,retry in "+ (1+6*(i>10?10:i))+"second");
	    Thread.sleep(1000+6000*(i>10?10:i));
	    etlNodeList = zk.getChildren(DATA_ROOT);
	    i++;
	}
	List<EtlItem> etlList = new ArrayList<EtlItem>();
	for( String item:etlNodeList){
	    String zkStr =zk.readData(DATA_ROOT+"/"+item);
	    etlList.add(Transform.Trans(zkStr));
	}
	int bestIndex = 0 ;
	float bestTime;
	i = 0;
	while( i < etlList.size() && etlList.get(i).getSpeed() == 0 ){
	    i++;
	}
	while(i>=etlList.size()){
	    System.out.println("all etlServer speed =0 ,reUpdate in 1 second");
	    Thread.sleep(1000);
	    etlNodeList = zk.getChildren(DATA_ROOT);
	    i=0;
	    while(etlNodeList.size() == 0 ){
		System.out.println(new SimpleDateFormat("yy-MM-dd hh:mm:ss").format(new Date())+"\tNo etlServer exist,retry in "+ (1+6*(i>10?10:i))+"second");
		Thread.sleep(1000+6000*(i>10?10:i));
		etlNodeList = zk.getChildren(DATA_ROOT);
		i++;
	    }
	    etlList = new ArrayList<EtlItem>();
	    for( String item:etlNodeList){
		String zkStr =zk.readData(DATA_ROOT+"/"+item);
		etlList.add(Transform.Trans(zkStr));
	    }
	    i = 0;
	    while( i < etlList.size() &&etlList.get(i).getSpeed() == 0){
		i++;
	    }
	}
	bestTime  = (float)((float)etlList.get(0).getCurTaskLength()/(float)etlList.get(i).getSpeed());
	float tryTime = 0;
	for( ; i < etlList.size() ; ++ i ){
	    if(etlList.get(i).getSpeed() == 0){
		continue;
	    }
	    tryTime = (float)((float)etlList.get(i).getCurTaskLength()/(float)etlList.get(i).getSpeed());
	    if(tryTime<bestTime){
		bestIndex = i ; 
		bestTime = tryTime;
	    }
	}
	taskItem.setEtlIp(etlList.get(bestIndex).getIp());
	taskItem.setEtlPort(etlList.get(bestIndex).getPort());
	Thread.sleep(100);
	return true;
    }
    private void ZKClose() throws InterruptedException{
	zk.close();
    }
    static public synchronized ScheduleOld getScheduleHandler() throws IOException, KeeperException, InterruptedException, Exception{
	if(schdl == null){
	    schdl = new ScheduleOld();
	}
	return schdl;
    }
    static public boolean destroyScheduleHandler() throws InterruptedException{
	if(schdl == null ){
	    return true;
	}
	schdl.ZKClose();
	schdl = null;
	return true;
    }
}
