/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.cc.slave.status;

import cn.ac.iie.cls.etl.cc.slave.etltask.ETLTaskTracker;
import cn.ac.iie.cls.etl.cc.slave.zkForm.EtlItem;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.KeeperException;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 *
 * @author L-R
 */
public class UpdateJob implements Job{

    
    private static enum specialTimeState{None,TaskPoolEmpty,TaskUndown};

    private static ZkClient zk;
    private static String nodePath;
    private static int taskDownNumInCurPeriod;
    private static Lock taskDownNumLock =new ReentrantLock();
    private static EtlItem lastState;
    
    public static boolean init(ZkClient zk , String nodePath,String IpPort){
	UpdateJob.zk = zk;
	UpdateJob.nodePath = nodePath;
	taskDownNumInCurPeriod=0;
	lastState = new EtlItem(IpPort.split(":")[0], Integer.valueOf(IpPort.split(":")[1]), taskDownNumInCurPeriod, 1, System.currentTimeMillis(),1000);
	return true;
    }
    
    @Override
    public void execute(JobExecutionContext jec) throws JobExecutionException {
        taskDownNumLock.lock();
        try{
            if(ETLTaskTracker.getEtlTaskLength() == 0 && taskDownNumInCurPeriod == 0){
               updateTime();
            }else{
                nomalUpdate(jec);
            }
        }
        catch(Exception ex){
            ex.printStackTrace();
        }
        finally{
            taskDownNumInCurPeriod = 0;
            taskDownNumLock.unlock();
        }
    }
    static private void nomalUpdate(JobExecutionContext jec) throws KeeperException, InterruptedException{
	lastState.setCurTaskLength(ETLTaskTracker.getEtlTaskLength());
	lastState.setSpeed(taskDownNumInCurPeriod);
	lastState.setUpdateTime(System.currentTimeMillis());
	if(zk.exists(nodePath)){
	    zk.writeData(nodePath, lastState.toString());
	}else{
	    zk.createEphemeral(nodePath, lastState.toString());
	}
	   
    }

    static private void updateTime() throws KeeperException, InterruptedException{
	lastState.setUpdateTime(System.currentTimeMillis());
	if(zk.exists(nodePath)){
	    zk.writeData(nodePath, lastState.toString());
	}else{
	    zk.createEphemeral(nodePath, lastState.toString());
	}
    }

    static public boolean newTaskDown(){
        taskDownNumLock.lock();
        try {
            taskDownNumInCurPeriod++;
        } finally {
            taskDownNumLock.unlock();
        }
        return true;
    }
    static public boolean newTaskDown(int taskNum){
        taskDownNumLock.lock();
        try {
            taskDownNumInCurPeriod+=taskNum;
        } finally {
            taskDownNumLock.unlock();
        }
        return true;
    }
}
