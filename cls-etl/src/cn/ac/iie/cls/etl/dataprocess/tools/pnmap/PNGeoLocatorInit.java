/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.tools.pnmap;

import cn.ac.ict.ncic.util.dao.DaoPool;
import cn.ac.iie.cls.etl.dataprocess.commons.RuntimeEnv;
import cn.ac.iie.cls.etl.dataprocess.config.Configuration;
import cn.ac.iie.cls.etl.dataprocess.operator.fieldoperator.PhoneNumberMapOperator;
import cn.ac.iie.cls.etl.dataprocess.util.rangesearch.RangeSearch;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author alexmu
 */
public class PNGeoLocatorInit {

    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(PNGeoLocatorInit.class.getName());
    }

    public static void main(String[] args) throws Exception {
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

        Map<String, RangeSearch> pnGeoLocator = new HashMap<String, RangeSearch>();
        ResultSet rs = null;
        try {
            String sql = "select start_phone_num,end_phone_num,province,district,isp,remark from dic_phone_location";
            rs = DaoPool.getDao(RuntimeEnv.METADB_CLUSTER).executeQuery(sql);
            RangeSearch range = null;
            String pnPrefix = null;
            int num = 0;
            while (rs.next()) {
                pnPrefix = rs.getString("start_phone_num").substring(0, PhoneNumberMapOperator.PREFIX_LENTH);
                range = pnGeoLocator.get(pnPrefix);
                if (range == null) {
                    range = new RangeSearch();
                    pnGeoLocator.put(pnPrefix, range);
                }
                range.append(rs.getLong("start_phone_num"), rs.getLong("end_phone_num"), "province", rs.getString("province"));
                range.append(rs.getLong("start_phone_num"), rs.getLong("end_phone_num"), "district", rs.getString("district"));
                range.append(rs.getLong("start_phone_num"), rs.getLong("end_phone_num"), "isp", rs.getString("isp"));
                range.append(rs.getLong("start_phone_num"), rs.getLong("end_phone_num"), "remark", rs.getString("remark"));
                num++;
                System.out.println(num);
            }
            Set pns = pnGeoLocator.keySet();
            Iterator itr = pns.iterator();
            while (itr.hasNext()) {
                pnGeoLocator.get(itr.next()).contructArray();
            }
            
            ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream("PNGeoLocator.dat")));
            out.writeObject(pnGeoLocator);
            out.close();
            logger.info("init pnGeoLocator successfully with " + num + " records");
        } catch (Exception ex) {
            pnGeoLocator = null;
            throw ex;
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
