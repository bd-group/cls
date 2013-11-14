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
public class StreamAlgorithmRandom implements StreamAlgorithmInterface{

    private final Random rd;

    public StreamAlgorithmRandom() {
	rd = new Random(Integer.valueOf(new SimpleDateFormat("mmddss").format(new Date())));
    }
    @Override
    public TaskItem Schedule(List<EtlItem> etlList) {
	int curEtlIndex = rd.nextInt(etlList.size());
	return new TaskItem(etlList.get(curEtlIndex).getIp(), etlList.get(curEtlIndex).getPort());
    }
    
}
