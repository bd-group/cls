/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.dataetl.schedule;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import org.I0Itec.zkclient.ZkClient;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 *
 * @author L_R
 */
public class AutoUpdateJob implements Job {

    private static ReentrantLock oneUpdatePerTimeLock = new ReentrantLock();

    @Override
    public void execute(JobExecutionContext jec) throws JobExecutionException {
        if (oneUpdatePerTimeLock.tryLock()) {
            try {
                ZkClient zk = ScheduleFactory.getZkClient();
                String CLSETL_ROOT = ScheduleFactory.getEtlServerRootPath();
                if (!zk.exists(CLSETL_ROOT)) {
                    System.err.println("can not find Etl State Node clsCCRoot Path");
                    return;
                }
                List<String> etlNodeList = zk.getChildren(CLSETL_ROOT + "/slaves");
                int i = 0;
                while (etlNodeList.isEmpty()) {
                    System.out.println(new SimpleDateFormat("yy-MM-dd hh:mm:ss").format(new Date()) + "\tNo etlServer exist,retry in " + (1 + 6 * (i > 10 ? 10 : i)) + "second");
                    try {
                        Thread.sleep(1000 + 6000 * (i > 10 ? 10 : i));
                    } catch (InterruptedException ex) {
                        System.err.println(ex.getMessage() + " ,Caused by :" + ex.getCause());
                    }
                    etlNodeList = zk.getChildren(CLSETL_ROOT + "/slaves");
                    i++;
                }
                List<EtlItem> etlList = new ArrayList<EtlItem>();
                for (String item : etlNodeList) {
                    String zkStr = zk.readData(CLSETL_ROOT + "/slaves/" + item);
                    etlList.add(Transform.Trans(zkStr));
                }
                ScheduleFactory.setEtlList(etlList);
            } finally {
                oneUpdatePerTimeLock.unlock();
            }
        } else {
            //wait for others to finishe update
            oneUpdatePerTimeLock.lock();
            oneUpdatePerTimeLock.unlock();
        }
    }
}
