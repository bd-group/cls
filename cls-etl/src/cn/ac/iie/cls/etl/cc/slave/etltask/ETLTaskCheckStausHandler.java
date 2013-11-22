package cn.ac.iie.cls.etl.cc.slave.etltask;

import cn.ac.iie.cls.etl.cc.slave.SlaveHandler;

public class ETLTaskCheckStausHandler implements SlaveHandler
{
	@Override
	public String execute(String pRequestContent)
	{
		System.out.println("CheckStatusRequestContent =========== ===== ===== = = = == ="+ pRequestContent);
		return ETLTaskTracker.checkTaskStatus(pRequestContent);
	}
}
