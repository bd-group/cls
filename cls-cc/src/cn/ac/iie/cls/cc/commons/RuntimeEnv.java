/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.commons;

import cn.ac.ict.ncic.util.dao.DaoPool;
import cn.ac.ict.ncic.util.dao.util.ClusterInfoOP;
import cn.ac.iie.cls.cc.config.Configuration;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author AlexMu
 */
public class RuntimeEnv {

    private static Configuration conf = null;
    private static final String DB_CLUSTERS = "dbClusters";
    public static final String METADB_CLUSTER = "metaDBCluster";
    public static final String HDFS_CONN_STR = "hdfsConnStr";
    public static final String HIVE_CONN_STR = "hiveConnStr";
    public static final String ZK_CLUSTER = "zkCluster";
    public static final String TMP_DATA_DIR = "tmpDataDir";
    public static final String SYSTEM_CC_ROOT_URI = "systemCCRootUri";
    private static Map<String, Object> dynamicParams = new HashMap<String, Object>();
    //logger
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(RuntimeEnv.class.getName());
    }

    public static boolean initialize(Configuration pConf) throws Exception {

        if (pConf == null) {
            logger.error("configuration object is null");
            throw new Exception("configuration object is null");
        }

        conf = pConf;

        String dbCluster = conf.getString(DB_CLUSTERS, "");
        if (dbCluster.isEmpty()) {
            logger.error("parameter dbcluster does not exist or is not defined");
            throw new Exception("parameter dbcluster does not exist or is not defined");
        }

        try {
            DaoPool.putDao(ClusterInfoOP.getDBClusters(dbCluster));//need check
        } catch (Exception ex) {
            logger.error("init dao is failed for " + ex);
            throw new Exception("init dao is failed for " + ex);
        }

        String hdfsConnStr = conf.getString(HDFS_CONN_STR, "");
        if (hdfsConnStr.isEmpty()) {
            logger.error("parameter hdfsConnStr does not exist or is not defined");
            throw new Exception("parameter hdfsConnStr does not exist or is not defined");
        }

        addParam(HDFS_CONN_STR, hdfsConnStr);


        String hiveConnStr = conf.getString(HIVE_CONN_STR, "");
        if (hiveConnStr.isEmpty()) {
            logger.error("parameter hiveConnStr does not exist or is not defined");
            throw new Exception("parameter hiveConnStr does not exist or is not defined");
        }

        addParam(HIVE_CONN_STR, hiveConnStr);

        String zkCluster = conf.getString(ZK_CLUSTER, "");
        if (zkCluster.isEmpty()) {
            logger.error("parameter zkCluster does not exist or is not defined");
            throw new Exception("parameter zkCluster does not exist or is not defined");
        }

        addParam(ZK_CLUSTER, zkCluster);

        String tmpDataDir = conf.getString(TMP_DATA_DIR, "");
        if (tmpDataDir.isEmpty()) {
            logger.error("parameter tmpDataDir does not exist or is not defined");
            throw new Exception("parameter tmpDataDir does not exist or is not defined");
        }

        addParam(TMP_DATA_DIR, tmpDataDir);

        String systemCCRootUri = conf.getString(SYSTEM_CC_ROOT_URI, "");
        if (systemCCRootUri.isEmpty()) {
            logger.error("parameter systemCCRootUri does not exist or is not defined");
            throw new Exception("parameter systemCCRootUri does not exist or is not defined");
        }

        addParam(SYSTEM_CC_ROOT_URI, systemCCRootUri);
        return true;
    }

    public static void dumpEnvironment() {
        conf.dumpConfiguration();
    }

    public static void addParam(String pParamName, Object pValue) {
        synchronized (dynamicParams) {
            dynamicParams.put(pParamName, pValue);
        }
    }

    public static Object getParam(String pParamName) {
        return dynamicParams.get(pParamName);
    }
}
