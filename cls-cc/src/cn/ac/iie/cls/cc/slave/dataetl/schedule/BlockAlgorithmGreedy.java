/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.dataetl.schedule;

import java.util.List;

/**
 *
 * @author L_R
 */
public class BlockAlgorithmGreedy implements BlockAlgorithmInterface{

    
    @Override
    public boolean Schedule(List<TaskItem> taskList, List<EtlItem> etlList) {
	//sort down task by length
	int []TaskIndex = new int[taskList.size()];
	for(int i = 0 ; i < TaskIndex.length ; ++ i){
	    TaskIndex[i] = i;
	}
	sortDownByLength(taskList, TaskIndex);
	//get current time & speed of etls 
	//if speed <=0 , etl server will not be usable
	float []Time = new float[etlList.size()];
	float []Speed = new float [etlList.size()];
	for(int i = 0  ;  i< Time.length ; ++ i)
	{
	    Speed[i] = etlList.get(i).getSpeed();
	    if(Speed[i] <= 0 ){
		Time[i] = -1;
	    }else{
		Time[i] = (float)etlList.get(i).getCurTaskLength() / (float)Speed[i];
	    }
	}
	int minFinishTimeIndex ;//[0,etlList.size()]
	//figure out the finish time with task[longest]
	for(int i = 0 ; i < Time.length ; ++ i){
	    if(Speed[i] > 0){
		Time[i] += (float)taskList.get(TaskIndex[0]).getTaskLength()/Speed[i];
	    }
	}
	//get schedule for task[longest]
	minFinishTimeIndex = 0 ;
	for(int i = 1 ; i < Time.length ; ++ i){
	    if(Time[i] < Time[minFinishTimeIndex]){
		minFinishTimeIndex = i;
	    }
	}
	taskList.get(TaskIndex[0]).setEtlIp(etlList.get(minFinishTimeIndex).getIp());
	taskList.get(TaskIndex[0]).setEtlPort(etlList.get(minFinishTimeIndex).getPort());
	Time[minFinishTimeIndex] += (float)taskList.get(TaskIndex[0]).getTaskLength()/Speed[minFinishTimeIndex];
	
	//figure & schedule task[i],with i = [1-max]
	for(int j = 1 ; j < TaskIndex.length; ++ j){
	    //figure out the finish time with task[i]
	    for(int i = 0 ; i < Time.length ; ++ i){
		if(Speed[i] > 0){
		    Time[i] += (float)(taskList.get(TaskIndex[j]).getTaskLength()-taskList.get(TaskIndex[j-1]).getTaskLength())/Speed[i];
		}
	    }
	    //get schedule for task[i]
	    minFinishTimeIndex = 0 ;
	    for(int i = 1 ; i < Time.length ; ++ i){
		if(Time[i] < Time[minFinishTimeIndex]){
		    minFinishTimeIndex = i;
		}
	    }
	    taskList.get(TaskIndex[j]).setEtlIp(etlList.get(minFinishTimeIndex).getIp());
	    taskList.get(TaskIndex[j]).setEtlPort(etlList.get(minFinishTimeIndex).getPort());
	    Time[minFinishTimeIndex] += (float)taskList.get(TaskIndex[0]).getTaskLength()/Speed[minFinishTimeIndex];
	}
	//update curTaskLen of etl Servers
	for(int i = 0 ; i < Time.length ; ++ i){
	    if(Speed[i]>0){
		etlList.get(i).setCurTaskLength((int)(Time[i]*Speed[i]-taskList.get(TaskIndex.length-1).getTaskLength()));
	    }
	}
	return true;
    }
        /*
     * 二分法
     */
    private boolean sortDownByLength(List<TaskItem>taskList, int [] TaskIndex){
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
	if(!sortDownByLength(taskList,left)){
	    return false;
	}
	if(!sortDownByLength(taskList,right)){
	    return false;
	}
	int leftIndex = 0 , rightIndex = 0;
	TaskIx = 0 ;
	while(leftIndex < left.length && rightIndex < right.length && TaskIx < TaskIndex.length){
	    if(taskList.get(left[leftIndex]).getTaskLength()>taskList.get(right[rightIndex]).getTaskLength()){
		TaskIndex[TaskIx ++ ] = left[leftIndex ++ ];
	    }else{
		TaskIndex[TaskIx ++ ] = right[rightIndex ++ ];
	    }
	}
	if(leftIndex ==left.length){
	    while(TaskIx<TaskIndex.length && rightIndex < right.length){
		TaskIndex[TaskIx ++ ] = right[rightIndex ++ ];
	    }
	    if(TaskIx == TaskIndex.length && rightIndex == right.length){
		return true;
	    }
	    return false;
	}
	if(rightIndex == right.length){
	    while(TaskIx<TaskIndex.length && leftIndex < left.length){
		TaskIndex[TaskIx ++ ] = left[leftIndex ++ ];
	    }
	    if(TaskIx == TaskIndex.length && leftIndex == left.length){
		return true;
	    }
	    return false;
	}
	return false;
    }
    
}
