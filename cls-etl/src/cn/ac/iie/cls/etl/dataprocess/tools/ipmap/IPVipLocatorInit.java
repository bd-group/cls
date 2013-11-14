/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.tools.ipmap;

import cn.ac.ict.ncic.util.dao.DaoPool;
import cn.ac.iie.cls.etl.dataprocess.commons.RuntimeEnv;
import cn.ac.iie.cls.etl.dataprocess.config.Configuration;
import cn.ac.iie.cls.etl.dataprocess.util.rangesearch.RangeSearch;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author alexmu
 */
public class IPVipLocatorInit {

    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(IPVipLocatorInit.class.getName());
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        // TODO code application logic here
        String configurationFileName = "cls-etl.properties";
        Configuration conf = Configuration.getConfiguration(configurationFileName);
        if (conf == null) {
            throw new Exception("reading " + configurationFileName + " is failed.");
        }

        logger.info("initializng runtime enviroment...");
        if (!RuntimeEnv.initialize(conf)) {
            throw new Exception("initializng runtime enviroment is failed");
        }
        logger.info("initialize runtime enviroment successfully");

        RangeSearch ipVipLocator = new RangeSearch();
        ipVipLocator = new RangeSearch();
        ResultSet rs = null;
        try {
            String sql = "select start_ip_n,end_ip_n,vip_id from dic_vip_ip";
            rs = DaoPool.getDao(RuntimeEnv.METADB_CLUSTER).executeQuery(sql);
            int num = 0;
            while (rs.next()) {
                ipVipLocator.append(rs.getLong("start_ip_n"), rs.getLong("end_ip_n"), rs.getString("vip_id"), rs.getString("vip_id"));
                num++;
                System.out.println(num);
            }
            ipVipLocator.contructArray();
            ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream("IPVipLocator.dat")));
            out.writeObject(ipVipLocator);
            out.close();
            logger.info("init ipVipLocator successfully with " + num + " records");
        } catch (Exception ex) {
            logger.warn("init ipVipLocator unsuccessfully for " + ex.getMessage(), ex);
            ipVipLocator = null;
        } finally {
            Connection tmpConn = null;
            try {
                tmpConn = rs.getStatement().getConnection();
            } catch (Exception ex) {
            }
            try {
                rs.close();
            } catch (Exception ex) {
            }
            try {
                tmpConn.close();
            } catch (Exception ex) {
            }
        }
    }
}
