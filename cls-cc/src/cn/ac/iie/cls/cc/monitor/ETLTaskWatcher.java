package cn.ac.iie.cls.cc.monitor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.I0Itec.zkclient.IZkChildListener;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import cn.ac.ict.ncic.util.dao.Dao;
import cn.ac.ict.ncic.util.dao.DaoPool;
import cn.ac.iie.cls.cc.commons.RuntimeEnv;
import cn.ac.iie.cls.cc.config.Configuration;
import cn.ac.iie.cls.cc.slave.dataetl.ETLJob;
import cn.ac.iie.cls.cc.slave.dataetl.ETLJobTracker;
import cn.ac.iie.cls.cc.slave.dataetl.ETLTask;

public class ETLTaskWatcher {

    private static String serverIP;
    private static String serverPort;
    private static String ccRoot;
    private static String etlRoot;
    static Logger logger = null;
    static int count = 0;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(MasterWatcher.class.getName());
    }

    public static void startWatching() throws Exception {
        if (!RuntimeEnv.zk.exists(etlRoot)) {
            System.out.println("existsss " + RuntimeEnv.zk.exists(etlRoot));
            try {
                RuntimeEnv.zk.createPersistent(etlRoot);
                RuntimeEnv.zk.createPersistent(etlRoot + "/task2recover");
                logger.info("root " + etlRoot + "/task2recover created");
            } catch (Exception e) {
                logger.info("root exists : other master has created the root", e);
            }
        }

        RuntimeEnv.zk.subscribeChildChanges(etlRoot + "/task2recover", new IZkChildListener() {
            @Override
            public synchronized void handleChildChange(String parentPath, List<String> currentChilds)
                    throws Exception {
                // TODO Auto-generated method stub
                logger.info("some tasks write to zookeeper because it can't commit to cc, starting to recover these tasks...");
                List<String> childs = RuntimeEnv.zk.getChildren(etlRoot + "/task2recover");

                int childSize = childs.size();
                if (childSize > 0) {
                    for (int i = 0; i < childSize; i++) {
                        String finishedTask = RuntimeEnv.zk.readData(childs.get(i));
                        //task exit | executeResult | jobId | taskId | filePath | dataProcessStat | etlIpport | needresend(boolean) | redoNum(int)
                        String[] reportItems = finishedTask.split("[|]");
                        String executeResult = reportItems[1];
                        String processJobInstanceId = reportItems[2];
                        String filePath = reportItems[4];
                        String taskId = reportItems[3];
                        int dispatchTimes = Integer.parseInt(reportItems[8]);
                        ETLJob etlJob = ETLJobTracker.getETLJobTracker().getJob(processJobInstanceId);
                        int failedTimes = etlJob.getFailedTime(taskId);
                        ETLTask etlTask = new ETLTask(filePath, ETLTask.ETLTaskStatus.ENQUEUE, reportItems[5], taskId, reportItems[6], dispatchTimes, failedTimes);
                        if (executeResult.equals("SUCCEEDED")) {
                            etlTask.setTaskStatus(ETLTask.ETLTaskStatus.SUCCEEDED);
                        } else if (executeResult.equals("HALFSUCCEEDED")) {
                            etlTask.setTaskStatus(ETLTask.ETLTaskStatus.HALFSUCCEED);
                        } else if (executeResult.equals("FAILED")) {
                            etlTask.setTaskStatus(ETLTask.ETLTaskStatus.FAILED);
                        }

                        if (dispatchTimes == 0) {
                            List<ETLTask> etlTaskList = new ArrayList<ETLTask>();
                            etlTaskList.add(etlTask);
                            ETLJobTracker.getETLJobTracker().responseTask(processJobInstanceId, etlTaskList);
                        } else {
                            ResultSet rs = null;
                            String sql = "";
                            Dao dao = DaoPool.getDao(RuntimeEnv.METADB_CLUSTER);

                            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                            String date = sdf.format(new Date());

                            try {
                                sql = "select * from dp_task where job_id = " + processJobInstanceId + " and task_id = " + taskId + "and (task_status = " + ETLTask.ETLTaskStatus.ENQUEUE + "or task_status = " + ETLTask.ETLTaskStatus.EXECUTING + ")";
                                rs = dao.executeQuery(sql);
                                //查找当前是否有状态为ENQUEUE和EXECUTING的任务
                                while (rs.next()) {
                                    String taskStatus = rs.getString("task_status");
                                    if (taskStatus.equals("ENQUEUE") || taskStatus.equals("EXECUTING")) {
                                        List<ETLTask> etlTaskList = new ArrayList<ETLTask>();
                                        etlTaskList.add(etlTask);
                                        ETLJobTracker.getETLJobTracker().responseTask(processJobInstanceId, etlTaskList);
                                    }
                                }
                            } catch (SQLException e) {
                                e.printStackTrace();
                            } catch (Exception e) {
                                e.printStackTrace();
                            } finally {
                                Connection conn = null;
                                try {
                                    conn = rs.getStatement().getConnection();
                                    conn.close();
                                    rs.close();
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }

                            RuntimeEnv.zk.delete(childs.get(i));
                        }
                    }
                }
                logger.info("these tasks recovered successfully...");
            }
        });
    }

    public static void init(Configuration conf) throws Exception {
        logger.info("initializing cls cc MasterWatcher...");
        serverIP = conf.getString("jettyServerIP", "");
        if (serverIP.isEmpty()) {
            logger.error("parameter serverIP does not exist or is not defined");
            throw new Exception("parameter serverIP does not exist or is not defined");
        }
        serverPort = conf.getString("jettyServerPort", "");
        if (serverPort.isEmpty()) {
            logger.error("parameter serverPort does not exist or is not defined");
            throw new Exception("parameter serverPort does not exist or is not defined");
        }
        ccRoot = conf.getString("clsCCRoot", "");
        if (ccRoot.isEmpty()) {
            logger.error("parameter ccRoot does not exist or is not defined");
            throw new Exception("parameter ccRoot does not exist or is not defined");
        }

        etlRoot = conf.getString("clsETLRoot", "");
        if (etlRoot.isEmpty()) {
            logger.error("parameter etlRoot does not exist or is not defined");
            throw new Exception("parameter etlRoot does not exist or is not defined");
        }

        logger.info("initialize cls cc Master successfully");
    }
}
