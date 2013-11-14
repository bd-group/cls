/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.dataetl.schedule;

import java.util.List;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author L_R
 */
public class StreamScheduleRealtimeUpdate implements StreamScheduleInterface{
    
    private StreamAlgorithmInterface Algorithm;
    //log
    private static Logger logger = null;
    static {
	PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(BlockScheduleAutoUpdate.class.getName());
    }
    
    private StreamScheduleRealtimeUpdate(){
	Algorithm = null;
    }
    
    public StreamScheduleRealtimeUpdate(ScheduleFactory.stdStreamAlgorithm algorithm) throws Exception {
	this();
	Algorithm = (StreamAlgorithmInterface) ScheduleFactory.stdStreamAlgorithmMap.get(algorithm).newInstance();
    }

    public StreamScheduleRealtimeUpdate(StreamAlgorithmInterface algorithm) throws Exception {
	this();
	Algorithm = algorithm;
    }
    
    @Override
    public synchronized TaskItem Schedule() {
	if(Algorithm == null){
	    logger.error("no algorithm was set!");
	    return null;
	}
	try {
	    new AutoUpdateJob().execute(null);
	    List<EtlItem> etlList = ScheduleFactory.getEtlList();
	    return Algorithm.Schedule( etlList);
	} catch (Exception ex) {
	    logger.error(ex.getMessage());
	    return null;
	}
    }

    @Override
    public StreamScheduleInterface setAlgorithm(StreamAlgorithmInterface algorithm) {
	Algorithm = algorithm;
	return this;
    }

    @Override
    public void Destruct() {
    }
    
}
