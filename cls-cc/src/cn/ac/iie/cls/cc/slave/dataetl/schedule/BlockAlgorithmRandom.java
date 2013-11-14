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
 * @author L_R
 */
public class BlockAlgorithmRandom implements BlockAlgorithmInterface{

    private final Random rd;

    public BlockAlgorithmRandom() {
	rd = new Random(Integer.valueOf(new SimpleDateFormat("mmddss").format(new Date())));
    }
    
    
    
    @Override
    public boolean Schedule(List<TaskItem> taskList, List<EtlItem> etlList) {
	int TaskNum  = taskList.size();
	int EtlNum = etlList.size();
	int curEtlIndex;
	for(int i = 0 ; i < TaskNum ; ++i){
	    curEtlIndex = rd.nextInt(EtlNum);
	    taskList.get(i).setEtlIp(etlList.get(curEtlIndex).getIp());
	    taskList.get(i).setEtlPort(etlList.get(curEtlIndex).getPort());
	    etlList.get(curEtlIndex).setCurTaskLength(etlList.get(curEtlIndex).getCurTaskLength()+taskList.get(i).getTaskLength());
	}
	return true;
    }
    
}
