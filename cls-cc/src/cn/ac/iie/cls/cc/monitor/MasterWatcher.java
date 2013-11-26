package cn.ac.iie.cls.cc.monitor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

public class MasterWatcher {

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
        System.out.println("existsss " + ccRoot + ", " + RuntimeEnv.zk.exists(ccRoot));
        if (!RuntimeEnv.zk.exists(ccRoot)) {
            System.out.println("existsss " + RuntimeEnv.zk.exists(ccRoot));
            try {
                RuntimeEnv.zk.createPersistent(ccRoot);
                RuntimeEnv.zk.createPersistent(ccRoot + "/master");
                logger.info("root " + ccRoot + "/master created");
            } catch (Exception e) {
                logger.info("root exists : other master has created the root", e);
            }
        }

        if (!RuntimeEnv.zk.exists(ccRoot + "/master")) {
            System.out.println("existsss " + RuntimeEnv.zk.exists(ccRoot));
            try {
                RuntimeEnv.zk.createPersistent(ccRoot + "/master");
                logger.info("root " + ccRoot + "/master created");
            } catch (Exception e) {
                logger.info("master node exists : other master has created the node", e);
            }
        }

        if (!RuntimeEnv.zk.exists(etlRoot)) {
            System.out.println("existsss " + RuntimeEnv.zk.exists(etlRoot));
            try {
                RuntimeEnv.zk.createPersistent(etlRoot);
                RuntimeEnv.zk.createPersistent(etlRoot + "/task2recover");
                logger.info("root " + etlRoot + "/task2recover created");
            } catch (Exception e) {
                logger.info("etlRoot+/task2recover node exists : other master has created the node", e);
            }
        }

        if (!RuntimeEnv.zk.exists(etlRoot + "/task2recover")) {
            System.out.println("existsss " + RuntimeEnv.zk.exists(etlRoot));
            try {
                RuntimeEnv.zk.createPersistent(etlRoot + "/task2recover");
                logger.info("root " + etlRoot + "/task2recover created");
            } catch (Exception e) {
                logger.info("etlRoot+/task2recover node exists : other master has created the node", e);
            }
        }

        System.out.println("lock exists " + RuntimeEnv.zk.exists(ccRoot + "/master/lock"));
        if (!RuntimeEnv.zk.exists(ccRoot + "/master/lock")) {
            try {
                RuntimeEnv.zk.createEphemeral(ccRoot + "/master/lock", serverIP + ":" + serverPort);
                recover();
                logger.info("new lock created");
            } catch (Exception e) {
                logger.info("lock exists : another server has created the lock", e);
            }
        }

        RuntimeEnv.zk.subscribeChildChanges(ccRoot + "/master", new IZkChildListener() {
            @Override
            public synchronized void handleChildChange(String parentPath, List<String> currentChilds)
                    throws Exception {
                // TODO Auto-generated method stub
                System.out.println("in lock exists " + RuntimeEnv.zk.exists(ccRoot + "/master/lock"));
                if (!RuntimeEnv.zk.exists(ccRoot + "/master/lock")) {
                    try {
                        logger.info("current master crashed starting to recover...");
                        RuntimeEnv.zk.createEphemeral(ccRoot + "/master/lock", serverIP + ":" + serverPort);
                        String ipPort = RuntimeEnv.zk.readData(ccRoot + "/master/lock", true);
                        if (ipPort != null && ipPort.equals(serverIP + ":" + serverPort)) {
                            recover();
                        }
                    } catch (Exception e) {
                        logger.info("lock exists : another server has created the lock", e);
                    }
                }
            }
        });
    }

    public static void recover() {
            logger.info("new lock created starting to recover data...");
            ResultSet rs = null;
            String sql = "";
            Dao dao = DaoPool.getDao(RuntimeEnv.METADB_CLUSTER);

            try {
                //fixme
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                String date = sdf.format(new Date());
                //sql = "select job_id, distinct task_id, count(distinct task_id) from dp_task where (task_status = '" + ETLTask.ETLTaskStatus.EXECUTING + "' or task_status = '" + ETLTask.ETLTaskStatus.ENQUEUE + "' or task_status = '" + ETLTask.ETLTaskStatus.RECOVER + "') and update_time < to_date('" + date + "', 'YYYY-MM-DD HH24:MI:SS')";
                sql = "select * from dp_task where (task_status = '" + ETLTask.ETLTaskStatus.EXECUTING + "' or task_status = '" + ETLTask.ETLTaskStatus.ENQUEUE + "' or task_status = '" + ETLTask.ETLTaskStatus.RECOVER + "') and update_time < to_date('" + date + "', 'YYYY-MM-DD HH24:MI:SS')";
                logger.info(sql);
                rs = dao.executeQuery(sql);
                //内map方便发现zk中已经有处理完的信息时删除相应内map中的信息
                Map<String, Map<String, ETLTask>> recoverMap = new HashMap<String, Map<String, ETLTask>>();
                while (rs.next()) {
                    String jobId = rs.getString("job_id");
                    String taskId = rs.getString("task_id");
                    String filePath = rs.getString("task_file_path");
                    String ipport = rs.getString("etlserver_id");
                    int dispatchTimes = rs.getInt("dispatch_times");
                    int failedTimes = rs.getInt("failed_times");
                    System.out.println("recovering task : job_id = " + jobId + " task_file_path = " + filePath);
                    if (!recoverMap.containsKey(jobId)) {
                        recoverMap.put(jobId, new HashMap<String, ETLTask>());
                    }
                    recoverMap.get(jobId).put(taskId, new ETLTask(filePath, ETLTask.ETLTaskStatus.EXECUTING, taskId, ipport, dispatchTimes, failedTimes));
                }

                if (!recoverMap.isEmpty()) {
                    sql = "update dp_task set task_status = '" + ETLTask.ETLTaskStatus.RECOVER + "', update_time = to_date('" + date + "', 'YYYY-MM-DD HH24:MI:SS') where (task_status = '" + ETLTask.ETLTaskStatus.EXECUTING + "' or task_status = '" + ETLTask.ETLTaskStatus.ENQUEUE + "') and update_time < to_date('" + date + "', 'YYYY-MM-DD HH24:MI:SS')";
                    logger.info(sql);
                    dao.executeUpdate(sql);
                    
                    //fix me
                    //读取zk查找在数据库操作中执行完的任务
                    List<String> childs = RuntimeEnv.zk.getChildren(etlRoot + "/task2recover");

                    if (childs.size() > 0) {
                        for (int i = 0; i < childs.size(); i++) {
                            String response = RuntimeEnv.zk.readData(etlRoot + "/task2recover/" + childs.get(i)).toString();
                            //task_exit|" + taskStatus + "|" + processJobInstanceID + "|" + taskId + "|" + filePath + "|" + dataProcessStat + "|" + StatusUpdate.ETL_IPPORT + "|" + true + "|" + redoTimes
                            String[] responInfo = response.split("[|]");
                            if (responInfo[1].equals("SUCCEEDED")) {
                                recoverMap.get(responInfo[2]).remove(responInfo[3]);
                                if (recoverMap.get(responInfo[2]).size() == 0) {
                                    recoverMap.remove(responInfo[2]);
                                }

                                date = sdf.format(new Date());
                                sql = "insert into dp_task values ('" + responInfo[3] + "', '" + responInfo[2] + "', '" + responInfo[4] + "','" + ETLTask.ETLTaskStatus.SUCCEEDED + "' ,'" + serverIP + ":" + serverPort + "', '" + responInfo[5] + "', " + responInfo[8] + ", to_date('" + date + "', 'YYYY-MM-DD HH24:MI:SS'))";
                                logger.info(sql);
                                dao.executeUpdate(sql);
                                sql = "update dp_task set task_status = '" + ETLTask.ETLTaskStatus.CRASHED + "', update_time = to_date('" + date + "', 'YYYY-MM-DD HH24:MI:SS') where job_id = '" + responInfo[2] + "' and task_id = '" + responInfo[3] + "' and task_status = '" + ETLTask.ETLTaskStatus.RECOVER + "'";
                                logger.info(sql);
                                dao.executeUpdate(sql);
                            } else if (responInfo[1].equals("HALFSUCCEEDED")) {
                                recoverMap.get(responInfo[2]).remove(responInfo[3]);
                                if (recoverMap.get(responInfo[2]).size() == 0) {
                                    recoverMap.remove(responInfo[2]);
                                }

                                date = sdf.format(new Date());
                                sql = "insert into dp_task values ('" + responInfo[3] + "', '" + responInfo[2] + "', '" + responInfo[4] + "','" + ETLTask.ETLTaskStatus.HALFSUCCEED + "' ,'" + serverIP + ":" + serverPort + "', '" + responInfo[5] + "', " + responInfo[8] + ", to_date('" + date + "', 'YYYY-MM-DD HH24:MI:SS'))";
                                logger.info(sql);
                                dao.executeUpdate(sql);
                                sql = "update dp_task set task_status = '" + ETLTask.ETLTaskStatus.CRASHED + "', update_time = to_date('" + date + "', 'YYYY-MM-DD HH24:MI:SS') where job_id = '" + responInfo[2] + "' and task_id = '" + responInfo[3] + "' and task_status = '" + ETLTask.ETLTaskStatus.RECOVER + "'";
                                logger.info(sql);
                                dao.executeUpdate(sql);
                            }
                            RuntimeEnv.zk.delete(etlRoot + "/task2recover/" + childs.get(i));
                        }
                    }

                    //循环等待etl把任务执行完，每2s询问一次，超过5s则重发任务
                                    /*while (recoverMap.size() > 0) {
                     Thread.sleep(2000);

                     Set<String> jobIdKeys = recoverMap.keySet();
                     Iterator<String> jobIdIt = jobIdKeys.iterator();

                     while (jobIdIt.hasNext()) {
                     String jobId = jobIdIt.next();
                     Map<String, ETLTask> recoverEtlMap = recoverMap.get(jobId);
                     Set<String> jobTaskIdKeys = recoverEtlMap.keySet();
                     Iterator<String> jobTaskIdIt = jobTaskIdKeys.iterator();

                     while (jobTaskIdIt.hasNext()) {
                     String taskId = jobTaskIdIt.next();
                     ETLTask task = recoverEtlMap.get(taskId);
                     System.out.println("ask etl whether task : job_id = " + jobId + " task_file_path = " + task.getFilePath() + " has completed");
                     //fixme
                     String ipport = task.getEtlIpPort();
                     if (ipport!=null && !ipport.isEmpty()) {
                     String result = askWhetherETLJobCompleted(task.getEtlIpPort(), jobId, task.getFilePath());
                     System.out.println("result ==== " + result);
                     if(result.equals("SUCCEEDED")) {
                     date = sdf.format(new Date());
                     sql = "insert into dp_task values ('" + task.getTaskId() + "', '" + jobId + "', '" + task.getFilePath() +"'," + ETLTask.ETLTaskStatus.SUCCEEDED + " ,'" + serverIP + ":" + serverPort + "\', to_date('" + date + "', 'YYYY-MM-DD HH24:MI:SS'))";
                     dao.executeUpdate(sql);
                     recoverEtlMap.remove(taskId);        //传的是引用，可以直接用recoverEtlMap来删除
                     } else if(result.equals("HALFSUCCEEDED")) {
                     date = sdf.format(new Date());
                     sql = "insert into dp_task values ('" + task.getTaskId() + "', '" + jobId + "', '" + task.getFilePath() +"'," + ETLTask.ETLTaskStatus.HALFSUCCEEDED + " ,'" + serverIP + ":" + serverPort + "\', to_date('" + date + "', 'YYYY-MM-DD HH24:MI:SS'))";
                     dao.executeUpdate(sql);
                     recoverEtlMap.remove(taskId);        //传的是引用，可以直接用recoverEtlMap来删除
                     }
                     }
                     }
                     }

                     long endTime = System.currentTimeMillis();
                     if (endTime - startTime >= 5000) {
                     break;
                     }
                     }*/

                    if (recoverMap.size() > 0) {
                        Set<String> jobIdKeys = recoverMap.keySet();
                        Iterator<String> jobIdIt = jobIdKeys.iterator();
                        ETLJobTracker ejt = ETLJobTracker.getETLJobTracker();

                        while (jobIdIt.hasNext()) {
                            String jobId = jobIdIt.next();
                            ETLJob job = getJobFromDB(jobId);
                            String descriptor = job.getDataProcessDescriptor().get(ETLJob.DATA_ETL_DESC);
                            if (descriptor != null && !descriptor.isEmpty()) {
                                Map<String, ETLTask> recoverEtlMap = recoverMap.get(jobId);
                                Set<String> taskIdKeys = recoverEtlMap.keySet();
                                Iterator<String> taskIdIt = taskIdKeys.iterator();
                                List<ETLTask> resendEtlList = new ArrayList<ETLTask>();
                                while (taskIdIt.hasNext()) {
                                    String taskId = taskIdIt.next();
                                    ETLTask etlTask = recoverEtlMap.get(taskId);
                                    //job.setFailedTime(taskId, etlTask.getFailedTimes());
                                    //job.setTaskDispatchTimes(taskId, etlTask.getDispatchTimes()-1);
                                    resendEtlList.add(etlTask);
                                }
                                job.appendTask(resendEtlList);
                                ejt.appendJob(job);
                            }
                        }
                        sql = "update dp_task set task_status = '" + ETLTask.ETLTaskStatus.CRASHED + "', update_time = to_date('" + date + "', 'YYYY-MM-DD HH24:MI:SS') where task_status = '" + ETLTask.ETLTaskStatus.RECOVER + "'";
                        logger.info(sql);
                        dao.executeUpdate(sql);
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
//                    conn.close();
//                    rs.close();
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
            logger.info("task recovered successfully");
    }

    /*public static String askWhetherETLJobCompleted(String ipport, String jobId, String taskId) {
        HttpClient httpClient = new DefaultHttpClient();
        HttpPost httppost = new HttpPost("http://" + ipport + "/resources/etltask/response");
        InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream((jobId + "_" + taskId).getBytes()), -1);

        reqEntity.setContentType("binary/octet-stream");
        reqEntity.setChunked(true);
        httppost.setEntity(reqEntity);
        System.out.println(httppost.getURI() + " from askWhetherETLJobCompleted");
        String completed = "";
        try {
            HttpResponse response = httpClient.execute(httppost);
            StatusLine sl = response.getStatusLine();
            if (sl.getStatusCode() == 200) {      //200 404
                String result = HttpResponseParser.getResponseContent(response);
                System.out.println("hhttphttpresult ====== == = = = = = " + result);
                completed = result.trim();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            httppost.releaseConnection();
        }
        System.out.println("completed ====== == = = = = = " + completed);
        return completed;
    }*/

    private static ETLJob getJobFromDB(String jobId) {
        ResultSet rs = null;
        String sql = "";
        Dao dao = DaoPool.getDao(RuntimeEnv.METADB_CLUSTER);
        ETLJob etlJob = ETLJob.getETLJob();
        
        try {
            sql = "select * from dp_job where job_id = '" + jobId + "'";
            logger.info(sql);
            rs = dao.executeQuery(sql);
            if (rs.next()) {
                etlJob.setETLJobDescriptor(rs.getString("etl_data_desc"));
                etlJob.setProcessJobInstanceID(jobId);
                etlJob.setTask2doNum(rs.getInt("task_num"));
                rs.getString("job_status");
                etlJob.setJobStatus(ETLJob.JobStatus.RUNNING);
                System.out.println("job_id = = = " + etlJob.getProcessJobInstanceID());
            }
            
            sql = "select count(distinct task_id) from dp_task where job_id = '" + jobId + "' and task_id not in (select task_id from dp_task where job_id = '" + jobId + "' and (task_status = '" + ETLTask.ETLTaskStatus.EXECUTING + "' or task_status = '" + ETLTask.ETLTaskStatus.RECOVER + "'))";
            logger.info(sql);
            rs = dao.executeQuery(sql);
            if (rs.next()) {
                etlJob.setTask2doNum(etlJob.getTask2doNum() - rs.getInt(1));
                System.out.println("task2doNum = = = " + etlJob.getTask2doNum());
            }
            sql  = "select * from dp_task where job_id = '" + jobId + "' and (task_status = '" + ETLTask.ETLTaskStatus.ERRORTASK + "' or task_status = '" + ETLTask.ETLTaskStatus.ABORT + "') and task_id not in "
                    + "(select distinct task_id from dp_task where job_id = '" + jobId + "' and (task_status = '" + ETLTask.ETLTaskStatus.SUCCEEDED + "' or task_status = '" + ETLTask.ETLTaskStatus.HALFSUCCEED + "'))";
            logger.info(sql);
            rs = dao.executeQuery(sql);
            System.out.println("execute finished");
            if (rs.next()) {
                System.out.println("get one");
                etlJob.setJobSucceedFalse();
//                String taskId = rs.getString("task_id");
//                //if (!doneTaskSet.containsKey(taskId)) {
//                ETLTask etlTask = new ETLTask(rs.getString("task_file_path"), ETLTask.ETLTaskStatus.ERRORTASK, rs.getString("dataprocess_stat"), taskId, rs.getString("etlserver_id"), rs.getInt("redo_times"));
//                    //doneTaskSet.put(taskId, etlTask);
//                etlJob.appendFailedTask(etlTask);
//                //}
            }
        } catch (Exception ex) {
            etlJob = null;
            ex.printStackTrace();
        } finally {
            Connection conn = null;
            try {
                conn = rs.getStatement().getConnection();
//                conn.close();
//                rs.close();
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
        System.out.println("returning............");
        return etlJob;
    }
    
    /*private static boolean getCurrentJobStatusFromDBisSuccess(String jobId) {
        ResultSet rs = null;
        String sql = "";
        Dao dao = DaoPool.getDao(RuntimeEnv.METADB_CLUSTER);

        sql = "select * from dp_task d1, dp_task d2 where d1.job_id = " + jobId + " and d1.job_id = d2.job_id and ((d1.task_status = 'ERRORTASK' or d1.task_status = 'ABORT') and  d2.task_id = d1.task_id and d2.task_id <> 'SUCCEEDED')";
        logger.info(sql);
        try {
            rs = dao.executeQuery(sql);

            if (rs.next()) {
                return false;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            Connection conn = null;
            try {
                conn = rs.getStatement().getConnection();
                conn.close();
                rs.close();
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
        return true;
    }*/
    
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
    
    
    
//    public void exeQuery(String sql,Object callback){
//        try{
//            ResultSet rs = ;
//            object.callback(rs);
//        }catch(Exception ex){
//            
//        }finally{
//            
//        }
//    }
}
