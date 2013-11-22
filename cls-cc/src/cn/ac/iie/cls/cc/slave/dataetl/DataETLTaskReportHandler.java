/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.dataetl;

import cn.ac.ict.ncic.util.dao.Dao;
import cn.ac.ict.ncic.util.dao.DaoPool;
import cn.ac.iie.cls.cc.commons.RuntimeEnv;
import cn.ac.iie.cls.cc.slave.SlaveHandler;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author alexmu
 */
public class DataETLTaskReportHandler implements SlaveHandler {

    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(DataETLTaskReportHandler.class.getName());
    }

    public String execute(String pRequestContent) {
        String result = "";
        logger.info("response from cls-etl " + pRequestContent);
        //task_exit(task_precommit) | executeResult | jobId | taskId | filePath | dataProcessStat | etlIpport | needresend(boolean) | redoNum(int)
        String[] reportItems = pRequestContent.split("[|]");

        if (reportItems[0].equals("task_exit")) {
            String executeResult = reportItems[1];
            String processJobInstanceId = reportItems[2];
            String filePath = reportItems[4];
            String taskId = reportItems[3];
            int dispatchTimes = Integer.parseInt(reportItems[8]);
            ETLJob etlJob = ETLJobTracker.getETLJobTracker().getJob(processJobInstanceId);
            int failedTimes = etlJob.getFailedTime(taskId);
            ETLTask etlTask = new ETLTask(filePath, ETLTask.ETLTaskStatus.ENQUEUE, reportItems[5], taskId, reportItems[6], dispatchTimes, failedTimes);
                if (executeResult.equals("SUCCEEDED")) {
                    etlTask.taskStatus = ETLTask.ETLTaskStatus.SUCCEEDED;
                } else if (executeResult.equals("HALFSUCCEEDED")) {
                    etlTask.taskStatus = ETLTask.ETLTaskStatus.HALFSUCCEED;
                } else if (executeResult.equals("FAILED")) {
                    etlTask.taskStatus = ETLTask.ETLTaskStatus.FAILED;
                }
                etlTask.taskStat = reportItems[5];
                etlTask.dispatchTimes = dispatchTimes;
            if (etlJob != null && etlJob.taskExists(etlTask.taskId)) {
                List<ETLTask> etlTaskList = new ArrayList<ETLTask>();
                etlTaskList.add(etlTask);
                ETLJobTracker.getETLJobTracker().responseTask(processJobInstanceId, etlTaskList);
            } else {
                String sql = "";
                Dao dao = DaoPool.getDao(RuntimeEnv.METADB_CLUSTER);

                try {
                    //查找当前是否有状态为ENQUEUE和EXECUTING的任务
                    sql = "update dp_task set task_status = '" + ETLTask.ETLTaskStatus.ABORT + "' where job_id = '" + processJobInstanceId + "' and task_id = '" + taskId + "' and redo_times = " + dispatchTimes + " and (task_status = '" + ETLTask.ETLTaskStatus.EXECUTING + "' or task_status = '" + ETLTask.ETLTaskStatus.ENQUEUE + "' or task_status = '" + ETLTask.ETLTaskStatus.TIMEOUT + "')";
                    logger.info(sql);
                    if (dao.executeUpdate(sql) > 0) {
                        logger.info("job not exists, set task status ABORT");
                    }
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
                
                logger.info("job not exists, can't report to cc");
            }
            //ETLTask etlTask = new ETLTask(filePath, ETLTask.ETLTaskStatus.ENQUEUE, reportItems[5], taskId, reportItems[6], redoTimes);
            //HALFSUCCEEDED/SUCCEEDED/FAILED

            logger.info("task exit task status reported to cc successfully taskId = " + taskId);
        } else if (reportItems[0].equals("task_precommit")) {
            //"task_precommit|" + processJobInstanceID +  "|" + taskId + "|" + redoTimes
            String processJobInstanceId = reportItems[1];
            String taskId = reportItems[2];
            int redoTimes = Integer.parseInt(reportItems[3]);

            ETLJob ejb = ETLJobTracker.getETLJobTracker().getJob(processJobInstanceId);
            if (ejb != null) {
                String sql = "";
                ResultSet rs = null;
                Dao dao = DaoPool.getDao(RuntimeEnv.METADB_CLUSTER);

                try {
                    //查找当前有没有处于预提交的任务
                    sql = "select * from dp_task where job_id = '" + processJobInstanceId + "' and task_id = '" + taskId + "' and task_status = '" + ETLTask.ETLTaskStatus.PRECOMMIT + "'"; 
                    rs = dao.executeQuery(sql);
                    //等待6秒
                    if (rs.next()) {
                        long startTime = System.currentTimeMillis();
                        while (true) {
                            Thread.sleep(2000);
                            rs = dao.executeQuery(sql);
                            if (!rs.next() || System.currentTimeMillis()-startTime > 6000) {
                                break;
                            }
                        }
                    }
                    
                    //设置为PRECOMMIT
                    sql = "update dp_task set task_status = '" + ETLTask.ETLTaskStatus.PRECOMMIT + "' where job_id = '" + processJobInstanceId + "' and task_id = '" + taskId + "' and redo_times = " + redoTimes + " and (task_status = '" + ETLTask.ETLTaskStatus.EXECUTING + "' or task_status = '" + ETLTask.ETLTaskStatus.TIMEOUT + "')";
                    logger.info(sql);
                    if (dao.executeUpdate(sql) > 0) {
                        result = "true";
                        logger.info("cc agreed etl to commit taskId = " + taskId);
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    Connection conn = null;
                    try {
                        conn = rs.getStatement().getConnection();
                    } catch (Exception e) {
                        logger.warn("errors exists in closing sql connection", e);
                    }
                    try {
                        rs.close();
                    } catch (Exception ex) {
                    }
                    try {
                        conn.close();
                    } catch (Exception ex) {
                    }
                }
            } else {
                String sql = "";
                Dao dao = DaoPool.getDao(RuntimeEnv.METADB_CLUSTER);

                try {
                    sql = "update dp_task set task_status = '" + ETLTask.ETLTaskStatus.ABORT + "' where job_id = '" + processJobInstanceId + "' and task_id = '" + taskId + "' and redo_times = " + redoTimes + " + and task_status = '" + ETLTask.ETLTaskStatus.EXECUTING + "'";
                    logger.info(sql);
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
            }
        }
        System.out.println("result is =====  ==== " + result);
        return result;
    }
}
