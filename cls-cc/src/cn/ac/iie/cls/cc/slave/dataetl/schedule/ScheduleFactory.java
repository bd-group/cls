/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.dataetl.schedule;

import cn.ac.iie.cls.cc.config.Configuration;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.quartz.SchedulerException;

/**
 * 
 * @author L_R
 */
public class ScheduleFactory
{

	// 超时时间
	private static int SESSION_TIMEOUT;
	// 主机地址
	private static String ZK_CLUSTER;
	// 数据根目录
	private static String CLSCC_ROOT;
	private static String CLSETL_ROOT;
	// localport
	private static int LOCAL_PORT;
	//
	private static int UPDATE_INTERVAL_SECOND;
	// log
	private static Logger logger = null;
	static {
		PropertyConfigurator.configure("log4j.properties");
		logger = Logger.getLogger(BlockScheduleAutoUpdate.class.getName());
	}
	// init-tag
	private static boolean EnvInfoNotInitialized = true;
	private static ZkClient zk;
	private static List<EtlItem> etlList4AutoUpdate;
	private static BlockScheduleInterface blockSchd = null;
	private static StreamScheduleInterface streamSchd = null;
	private static Lock scheduleHandlerLock = new ReentrantLock();
	private static Lock envInfoInitLock = new ReentrantLock();

	public static enum UpdateMode {
		RealTime, Auto
	};

	public static enum stdBlockAlgorithm {
		Greedy, Random
	};

	public static enum stdStreamAlgorithm {
		Greedy, Random
	};

	public static final Map<stdBlockAlgorithm, Class> stdBlockAlgorithmMap = new EnumMap<stdBlockAlgorithm, Class>(
			stdBlockAlgorithm.class) {
		{
			put(stdBlockAlgorithm.Greedy, BlockAlgorithmGreedy.class);
			put(stdBlockAlgorithm.Random, BlockAlgorithmRandom.class);
		}
	};
	public static final Map<stdStreamAlgorithm, Class> stdStreamAlgorithmMap = new EnumMap<stdStreamAlgorithm, Class>(
			stdStreamAlgorithm.class) {
		{
			put(stdStreamAlgorithm.Greedy, StreamAlgorithmGreedy.class);
			put(stdStreamAlgorithm.Random, StreamAlgorithmRandom.class);
		}
	};

	public static List<EtlItem> getEtlList() {
		return etlList4AutoUpdate;
	}

	public static boolean setEtlList(List<EtlItem> newList) {
		etlList4AutoUpdate = newList;
		return true;
	}
        
        public static EtlItem removeETLItem (String ipport) {
            for (int i=0; i<etlList4AutoUpdate.size(); i++) {
                EtlItem eit = etlList4AutoUpdate.get(i);
                if ((eit.getIp() + ":" + eit.getPort()).equals(ipport)) {
                    etlList4AutoUpdate.remove(i);
                    return eit;
                }
            }
            return null;
        }

	public static ZkClient getZkClient() {
		return zk;
	}

	public static String getEtlServerRootPath() {
		return CLSETL_ROOT;
	}

	public static int getUpdateIntervalSecond() {
		return UPDATE_INTERVAL_SECOND;
	}

	public static BlockScheduleInterface getBlockScheduleHandler(
			UpdateMode updateMode, stdBlockAlgorithm algorithm)
			throws Exception {
		envInfoInitLock.lock();
		try {
			if (EnvInfoNotInitialized) {
				initEnvInfo();
			}
		} finally {
			envInfoInitLock.unlock();
		}
		scheduleHandlerLock.lock();
		try {
			if (streamSchd != null) {
				streamSchd.Destruct();
				streamSchd = null;
			}
			if (blockSchd != null) {
				blockSchd
						.setAlgorithm((BlockAlgorithmInterface) stdBlockAlgorithmMap
								.get(algorithm).newInstance());
			} else {
				blockSchd = UpdateMode.RealTime == updateMode ? new BlockScheduleRealtimeUpdate(
						algorithm) : new BlockScheduleAutoUpdate(algorithm);
			}
		} finally {
			scheduleHandlerLock.unlock();
			return blockSchd;
		}
	}

	public static BlockScheduleInterface getBlockScheduleHandler(
			UpdateMode updateMode, BlockAlgorithmInterface algrithm)
			throws Exception {
		envInfoInitLock.lock();
		try {
			if (EnvInfoNotInitialized) {
				initEnvInfo();
			}
		} finally {
			envInfoInitLock.unlock();
		}
		scheduleHandlerLock.lock();
		try {
			if (streamSchd != null) {
				streamSchd.Destruct();
				streamSchd = null;
			}
			if (blockSchd != null) {
				blockSchd.setAlgorithm(algrithm);
			} else {
				blockSchd = UpdateMode.RealTime == updateMode ? new BlockScheduleRealtimeUpdate(
						algrithm) : new BlockScheduleAutoUpdate(algrithm);
			}
		} finally {
			scheduleHandlerLock.unlock();
			return blockSchd;
		}
	}

	public static StreamScheduleInterface getStreamScheduleHandler(
			UpdateMode updateMode, stdStreamAlgorithm algorithm)
			throws Exception {
		envInfoInitLock.lock();
		try {
			if (EnvInfoNotInitialized) {
				initEnvInfo();
			}
		} finally {
			envInfoInitLock.unlock();
		}
		scheduleHandlerLock.lock();
		try {
			if (blockSchd != null) {
				blockSchd.Destruct();
				blockSchd = null;
			}
			if (streamSchd != null) {
				streamSchd
						.setAlgorithm((StreamAlgorithmInterface) stdStreamAlgorithmMap
								.get(algorithm).newInstance());
			} else {
				streamSchd = UpdateMode.RealTime == updateMode ? new StreamScheduleRealtimeUpdate(
						algorithm) : new StreamScheduleAutoUpdate(algorithm);
			}
		} finally {
			scheduleHandlerLock.unlock();
			return streamSchd;
		}
	}

	public static StreamScheduleInterface getStreamScheduleHandler(
			UpdateMode updateMode, StreamAlgorithmInterface algorithm)
			throws Exception {
		envInfoInitLock.lock();
		try {
			if (EnvInfoNotInitialized) {
				initEnvInfo();
			}
		} finally {
			envInfoInitLock.unlock();
		}
		scheduleHandlerLock.lock();
		try {
			if (blockSchd != null) {
				blockSchd.Destruct();
				blockSchd = null;
			}
			if (streamSchd != null) {
				streamSchd.setAlgorithm(algorithm);
			} else {
				streamSchd = UpdateMode.RealTime == updateMode ? new StreamScheduleRealtimeUpdate(
						algorithm) : new StreamScheduleAutoUpdate(algorithm);
			}
		} finally {
			scheduleHandlerLock.unlock();
			return streamSchd;
		}
	}

	private static boolean initEnvInfo() throws Exception {
		String configurationFileName = "cls-cc.properties";
		logger.info("initializing environment infomatinon...");
		logger.info("getting configuration from configuration file "
				+ configurationFileName);
		Configuration conf = Configuration
				.getConfiguration(configurationFileName);
		if (conf == null) {
			throw new Exception("reading " + configurationFileName
					+ " is failed.");
		}
		ZK_CLUSTER = conf.getString("zkCluster", "");
		if (ZK_CLUSTER.isEmpty()) {
			throw new Exception("definition zkCluster is not fond in"
					+ configurationFileName);
		}
		SESSION_TIMEOUT = conf.getInt("zkSessionTimeout", -1);
		if (SESSION_TIMEOUT == -1) {
			throw new Exception("definition zkSessionTimeout is not fond in"
					+ configurationFileName);
		}
		CLSCC_ROOT = conf.getString("clsCCRoot", "");
		if (CLSCC_ROOT.isEmpty()) {
			throw new Exception("definition clsCCRoot is not fond in"
					+ configurationFileName);
		}
		CLSETL_ROOT = conf.getString("clsETLRoot", "");
		if (CLSETL_ROOT.isEmpty()) {
			throw new Exception("definition clsETLRoot is not fond in"
					+ configurationFileName);
		}
		LOCAL_PORT = conf.getInt("zkLocalPort", -1);
		if (LOCAL_PORT == -1) {
			throw new Exception("definition zkLocalPort is not fond in"
					+ configurationFileName);
		}
		UPDATE_INTERVAL_SECOND = conf.getInt("updateIntervalSecond", -1);
		if (UPDATE_INTERVAL_SECOND == -1) {
			throw new Exception(
					"definition updateTntervalSecond is not fond in"
							+ configurationFileName);
		}
		logger.info("initialize environment infomaion succussfully !");

		zk = new ZkClient(ZK_CLUSTER, SESSION_TIMEOUT, SESSION_TIMEOUT + 1);
		EnvInfoNotInitialized = false;
		return true;
	}
}
