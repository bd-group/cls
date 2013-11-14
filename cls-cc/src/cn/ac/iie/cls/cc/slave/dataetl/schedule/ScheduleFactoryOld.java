/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.dataetl.schedule;

import java.util.List;


/**
 *
 * @author L-R
 */
public class ScheduleFactoryOld
{
    static public  ScheduleInterfaceOld getScedule(List<TaskItem> taskList, List<EtlItem> etlList)
    {
//	return new RandScheduleOld(taskList, etlList);
	return new GreedyScheduleOld(taskList, etlList);
	
	
    }
    static public ScheduleInterfaceOld getSchedule(List<TaskItem> taskList , List<EtlItem> etlList ,int algorithm){
	switch(algorithm){
	    case 0 :
		return new RandScheduleOld(taskList, etlList);
	    case 1:
		return new GreedyScheduleOld(taskList, etlList);
	    default:
		return null;
	}
    }

}
