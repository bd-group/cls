/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.dataetl.schedule;


import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 *
 * @author L-R
 */
public class RandScheduleOld implements ScheduleInterfaceOld
{
    private List<TaskItem> taskList;
    private List<EtlItem> etlList;

    public RandScheduleOld(List<TaskItem> taskList, List<EtlItem> etlList)
    {
	this.taskList = taskList;
	this.etlList = etlList;
    }
    
    @Override
    public boolean Schedule()
    {
	Random rd = new Random(Integer.valueOf(new SimpleDateFormat("mmddss").format(new Date())));
	int t = taskList.size();
	int max = etlList.size();
	for(int i = 0  ;i < t ; ++ i)
	{
	    taskList.get(i).setEtlIp(etlList.get(rd.nextInt(max)).getIp());
	    taskList.get(i).setEtlPort(etlList.get(rd.nextInt(max)).getPort());
	}
	return true;
    }

}
