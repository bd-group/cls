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
public class IPGeoLocatorInit {
    
    static Logger logger = null;
    
    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(IPGeoLocatorInit.class.getName());
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
        
        RangeSearch ipGeoLocator = new RangeSearch();
        ResultSet rs = null;
        try {
            String sql = "select start_ip_n,end_ip_n,country,district,isp from dic_ip_location";
            while (true) {
                try {
                    rs = DaoPool.getDao(RuntimeEnv.METADB_CLUSTER).executeQuery(sql);
                    break;
                } catch (Exception ex) {
                    continue;
                }
            }
            int num = 0;
            while (rs.next()) {
                long startIPN = rs.getLong("start_ip_n");
                long endIPN = rs.getLong("end_ip_n");
                ipGeoLocator.append(startIPN, endIPN, "country", rs.getString("country"));
                ipGeoLocator.append(startIPN, endIPN, "district", rs.getString("district"));
                ipGeoLocator.append(startIPN, endIPN, "isp", rs.getString("isp"));
                num++;
                System.out.println(num);
            }
            logger.info("init ipGeoLocator successfully with " + num + " records");
            ipGeoLocator.contructArray();
            
            ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream("IPGeoLocator.dat")));
            out.writeObject(ipGeoLocator);
            out.close();
        } catch (Exception ex) {
            logger.warn("init ipGeoLocator unsuccessfully for " + ex.getMessage(), ex);
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
