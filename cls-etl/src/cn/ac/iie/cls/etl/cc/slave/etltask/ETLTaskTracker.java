/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.cc.slave.etltask;

import cn.ac.iie.cls.etl.cc.slave.status.StatusUpdate;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 *
 * @author alexmu
 */
public class ETLTaskTracker implements Runnable {

    private static BlockingQueue<ETLTask> etlTaskWaitingList = new LinkedBlockingQueue<ETLTask>(1500);
    private static Map<String, ETLTask> etlTaskSet = new HashMap<String, ETLTask>();
    private static ETLTaskTracker etlTaskTracker = null;

    private ETLTaskTracker() {
    }

    public static synchronized ETLTaskTracker getETLTaskTracker() {
        if (etlTaskTracker == null) {
            etlTaskTracker = new ETLTaskTracker();
            Thread etlTaskTrackerRunner = new Thread(etlTaskTracker);
            etlTaskTrackerRunner.start();
        }
        return etlTaskTracker;
    }

    public void appendTask(ETLTask pETLTask) {
        try {
	    etlTaskWaitingList.put(pETLTask);
//	    StatusUpdate.updateState(etlTaskWaitingList.size()+etlTaskSet.size(),StatusUpdate.updateType.TaskAdd);
        } catch (Exception ex) {
        }
    }

    public void removeTask(ETLTask pETLTask) {
        synchronized (etlTaskSet) {
            etlTaskSet.remove(pETLTask.getID());
	    StatusUpdate.newTaskDown();
        }
    }

    @Override
    public void run() {
        ETLTask etlTask = null;
        while (true) {
            try {
                etlTask = etlTaskWaitingList.take();
                Thread etlTaskRunner = new Thread(etlTask);
                etlTaskRunner.start();
                etlTaskSet.put(etlTask.getID(), etlTask);
	//	Thread.sleep(600);
            } catch (Exception ex) {
            }
        }
    }
    
    public static int getEtlTaskLength(){
	return etlTaskWaitingList.size()+etlTaskSet.size();
    }
}
