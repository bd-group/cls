/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.dataetl.schedule;

import cn.ac.iie.cls.cc.config.Configuration;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

/**
 * 
 * @author L_R
 */
public class BlockScheduleRealtimeUpdate implements BlockScheduleInterface
{
	// log
	private static Logger logger = null;
	static {
		PropertyConfigurator.configure("log4j.properties");
		logger = Logger.getLogger(BlockScheduleAutoUpdate.class.getName());
	}

	private BlockAlgorithmInterface Algorithm;

	private BlockScheduleRealtimeUpdate() throws Exception {
		Algorithm = null;
	}

	public BlockScheduleRealtimeUpdate(
			ScheduleFactory.stdBlockAlgorithm algorithm) throws Exception {
		this();
		Algorithm = (BlockAlgorithmInterface) ScheduleFactory.stdBlockAlgorithmMap
				.get(algorithm).newInstance();
	}

	public BlockScheduleRealtimeUpdate(BlockAlgorithmInterface algrithm)
			throws Exception {
		this();
		Algorithm = algrithm;
	}

	@Override
	public synchronized boolean Schedule(List<TaskItem> taskList) {
		if (Algorithm == null) {
			logger.error("no algorithm was set!");
			return false;
		}
		try {
			List<EtlItem> etlList = getEtlServerStat();
			return Algorithm.Schedule(taskList, etlList);
		} catch (Exception ex) {
			System.err.println(ex.getMessage());
			return false;
		}
	}

	private List<EtlItem> getEtlServerStat() throws Exception {
		new AutoUpdateJob().execute(null);
		return ScheduleFactory.getEtlList();
	}

	@Override
	public BlockScheduleInterface setAlgorithm(BlockAlgorithmInterface algorithm) {
		Algorithm = algorithm;
		return this;
	}

	@Override
	public void Destruct() {
	}

}
