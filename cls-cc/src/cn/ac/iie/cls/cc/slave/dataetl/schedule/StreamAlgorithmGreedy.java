/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.dataetl.schedule;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author L_R
 */
public class StreamAlgorithmGreedy implements StreamAlgorithmInterface{

    @Override
    public TaskItem Schedule(List<EtlItem> etlList) {
	int bestIndex = 0;
	float minTime = 0;
	for(int i = 0 ; i < etlList.size() ; ++i){
	    if(etlList.get(i).getSpeed() > 0){
		bestIndex = i ; 
		minTime = (float)etlList.get(i).getCurTaskLength() / (float)etlList.get(i).getSpeed();
		break;
	    }
	}
	float tmpTime;
	for(int i = bestIndex + 1 ; i < etlList.size()  ; ++ i){
	    if(etlList.get(i).getSpeed() > 0){
		tmpTime = (float)etlList.get(i).getCurTaskLength()/(float)etlList.get(i).getSpeed();
		if(tmpTime<minTime || (tmpTime == minTime  && etlList.get(i).getSpeed() > etlList.get(bestIndex).getSpeed())){
		    minTime = tmpTime;
		    bestIndex = i ;
		}
	    }
	}
	synchronized(etlList){
	    etlList.get(bestIndex).setCurTaskLength(etlList.get(bestIndex).getCurTaskLength()+1);
	}
	return new TaskItem(etlList.get(bestIndex).getIp(), etlList.get(bestIndex).getPort());
    }
    
}
