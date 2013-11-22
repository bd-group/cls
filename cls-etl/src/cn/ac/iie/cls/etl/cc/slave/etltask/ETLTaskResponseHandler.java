package cn.ac.iie.cls.etl.cc.slave.etltask;

import cn.ac.iie.cls.etl.cc.slave.SlaveHandler;

public class ETLTaskResponseHandler implements SlaveHandler{

	@Override
	public String execute(String pRequestContent)
	{
		System.out.println("pRequestContent =========== ===== ===== = = = == ="+ pRequestContent);
		String taskStatus = ETLTaskTracker.checkTaskStatus(pRequestContent);
		return taskStatus;
	}
}
