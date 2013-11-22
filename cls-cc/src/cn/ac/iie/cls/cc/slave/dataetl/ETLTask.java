/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.dataetl;

/**
 *
 * @author alexmu
 */
public class ETLTask {

    public enum ETLTaskStatus {
        ENQUEUE, EXECUTING, PRECOMMIT, SUCCEEDED, HALFSUCCEED, FAILED, TIMEOUT, ABORT, ERRORTASK, RECOVER, CRASHED
    }
//	public static final int ENQUEUE = 0;
//	public static final int EXECUTING = 1;
//	public static final int PRECOMMIT = 2;
//    public static final int SUCCEEDED = 3;
//    public static final int HALFSUCCEEDED = -1;
//    public static final int FAILED = -2;
//    public static final int CRASHED = -3;
//    public static final int ABORT = -4;
//    public static final int ERRORTASK = -5;
    String filePath;
    ETLTaskStatus taskStatus;
    String taskStat;
    String taskId;
    int dispatchTimes = 0;
    int failedTimes = 0;
    String etlIpPort;

    private ETLTask(String pFileName) {
        this(pFileName, ETLTaskStatus.EXECUTING);
    }

    public ETLTask(String pFileName, ETLTaskStatus pTaskStatus) {
        this(pFileName, pTaskStatus, "", "", "", 0, 0);
    }
    
    public ETLTask(String pFileName, ETLTaskStatus pTaskStatus, String pTaskId) {
        this(pFileName, pTaskStatus, "", pTaskId,"", 0, 0);
    }
    
    public ETLTask(String pFileName, ETLTaskStatus pTaskStatus, String pTaskId, String pEtlIpPort, int pDispatchTimes, int pFailedTimes) {
    	this(pFileName, pTaskStatus, "", pTaskId, pEtlIpPort, pDispatchTimes, pFailedTimes);
    }

    public ETLTask(String pFileName, ETLTaskStatus pTaskStatus, String pTaskStat, String pTaskId, String pEtlIpPort, int pDispatchTimes, int pFailedTimes) {
        filePath = pFileName;
        taskStatus = pTaskStatus;
        taskStat = pTaskStat;
        taskId = pTaskId;
        etlIpPort = pEtlIpPort;
        dispatchTimes = pDispatchTimes;
        failedTimes = pFailedTimes;
    }

	public String getFilePath()
	{
		return filePath;
	}

	public String getTaskId()
	{
		return taskId;
	}

	public String getEtlIpPort()
	{
		return etlIpPort;
	}

        public int getDispatchTimes() {
            return dispatchTimes;
        }

        public int getFailedTimes() {
            return failedTimes;
        }
        
	public void setFailedTimes(int failedTimes)
	{
		this.failedTimes = failedTimes;
	}

	public ETLTaskStatus getTaskStatus()
	{
		return taskStatus;
	}

	public void setTaskStatus(ETLTaskStatus taskStatus)
	{
		this.taskStatus = taskStatus;
	}

	public String getTaskStat()
	{
		return taskStat;
	}

	public void setTaskStat(String taskStat)
	{
		this.taskStat = taskStat;
	}

	public void setFilePath(String filePath)
	{
		this.filePath = filePath;
	}

	public void setTaskId(String taskId)
	{
		this.taskId = taskId;
	}

	public void setEtlIpPort(String etlIpPort)
	{
		this.etlIpPort = etlIpPort;
	}
        
        public void setDispatchTimes(int dispatchTimes) {
            this.dispatchTimes = dispatchTimes;
        }
}
