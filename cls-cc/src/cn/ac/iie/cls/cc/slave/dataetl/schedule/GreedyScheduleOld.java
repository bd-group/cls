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
public class GreedyScheduleOld implements ScheduleInterfaceOld
{
    private List<TaskItem> taskList;
    private List<EtlItem> etlList;

    public GreedyScheduleOld(List<TaskItem> taskList, List<EtlItem> etlList)
    {
	this.taskList = taskList;
	this.etlList = etlList;
    }    
    @Override
    public boolean Schedule() {
	float []Time = new float[etlList.size()];
	float []Speed = new float [etlList.size()];
	for(int i = 0  ;  i< Time.length ; ++ i)
	{
	    Speed[i] = etlList.get(i).getSpeed();
	    Time[i] = (float)etlList.get(i).getCurTaskLength() / (float)Speed[i];
	}
	
	int []TaskIndex = new int[taskList.size()];
	for(int i = 0 ; i < TaskIndex.length ; ++ i){
	    TaskIndex[i] = i;
	}
	sortDownByLength(TaskIndex);
	

	int minTimeIndex ;//[0,etlList.size()]
	for(int i = 0 ; i < TaskIndex.length; ++ i){
	    minTimeIndex = 0 ;
	    for(int j = 1 ; j < Time.length ;  ++ j){
		if(Time[minTimeIndex] > Time[j]){
		    minTimeIndex = j ;
		}
	    }
	    taskList.get(TaskIndex[i]).setEtlIp(etlList.get(minTimeIndex).getIp());
	    taskList.get(TaskIndex[i]).setEtlPort(etlList.get(minTimeIndex).getPort());
	    Time[minTimeIndex]+=(float)taskList.get(TaskIndex[i]).getTaskLength()/(float)Speed[minTimeIndex];
	}
	for(int i = 0 ; i< etlList.size() ; ++ i)
	{
	    System.out.println(etlList.get(i).toString()+" time:"+Time[i]);
	}
	
	return true;
    }
    /*
     * 二分法
     */
    private boolean sortDownByLength(int [] TaskIndex){
	if(TaskIndex.length <= 1 ){
	    return true;
	}
	if(TaskIndex.length == 2){
	    if(taskList.get(TaskIndex[0]).getTaskLength()< taskList.get(TaskIndex[1]).getTaskLength()){
		int tmp = TaskIndex[0];
		TaskIndex[0] = TaskIndex[1];
		TaskIndex[1] = tmp;
	    }
	    return true;
	}
	
	int[]left = new int[TaskIndex.length/2];
	int[]right = new int[TaskIndex.length-left.length];
	for(int i = 0 ; i < left.length ; ++ i){
	    left[i] = TaskIndex[i];
	}
	int TaskIx = left.length;
	for(int i = 0 ; i < right.length ; ++ i){
	    right[i] = TaskIndex[TaskIx++];
	}
	sortDownByLength(left);
	sortDownByLength(right);
	int leftIndex = 0 , rightIndex = 0;
	TaskIx = 0 ;
	while(leftIndex < left.length && rightIndex < right.length && TaskIx < TaskIndex.length){
	    if(taskList.get(left[leftIndex]).getTaskLength()>taskList.get(right[rightIndex]).getTaskLength()){
		TaskIndex[TaskIx ++ ] = left[leftIndex ++ ];
	    }else{
		TaskIndex[TaskIx ++ ] = right[rightIndex ++ ];
	    }
	}
	if(TaskIx == TaskIndex.length){
	    if(leftIndex == left.length && rightIndex == right.length){
		return true;
	    }
	    return false;
	}
	if(leftIndex ==left.length){
	    while(TaskIx<TaskIndex.length){
		TaskIndex[TaskIx ++ ] = right[rightIndex ++ ];
	    }
	    if(rightIndex == right.length){
		return true;
	    }
	    return false;
	}
	if(rightIndex == right.length){
	    while(TaskIx<TaskIndex.length){
		TaskIndex[TaskIx ++ ] = left[leftIndex ++ ];
	    }
	    if(leftIndex == left.length){
		return true;
	    }
	    return false;
	}
	return false;
    }
    
}
