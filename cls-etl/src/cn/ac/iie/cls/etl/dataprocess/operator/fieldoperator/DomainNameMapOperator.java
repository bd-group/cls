/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator.fieldoperator;

import cn.ac.ict.ncic.util.dao.DaoPool;
import cn.ac.iie.cls.etl.dataprocess.commons.RuntimeEnv;
import cn.ac.iie.cls.etl.dataprocess.config.Configuration;
import cn.ac.iie.cls.etl.dataprocess.dataset.DataSet;
import cn.ac.iie.cls.etl.dataprocess.dataset.Field;
import cn.ac.iie.cls.etl.dataprocess.dataset.Record;
import cn.ac.iie.cls.etl.dataprocess.dataset.StringField;
import cn.ac.iie.cls.etl.dataprocess.operator.Operator;
import static cn.ac.iie.cls.etl.dataprocess.operator.Operator.FAILED;
import static cn.ac.iie.cls.etl.dataprocess.operator.Operator.SUCCEEDED;
import cn.ac.iie.cls.etl.dataprocess.operator.Port;
import cn.ac.iie.cls.etl.dataprocess.util.domaintreesearch.SubDomainNameMatch;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

/**
 *
 * @author alexmu
 */
public class DomainNameMapOperator extends Operator {

    public static final String IN_PORT = "inport1";
    public static final String OUT_PORT = "outport1";
    public static final String ERROR_PORT = "error1";
    List<Field2DNMap> field2DNMapSet = new ArrayList<Field2DNMap>();
    //
    private static Map<String, String> dnGeoLocator = null;
    private static Lock dnGeoLocatorLock = new ReentrantLock();
    private static Map<String, String> dnCategoryLocator = null;
    private static Lock dnCategoryLocatorLock = new ReentrantLock();
    private static Map<String, String> dnVipLocator = null;
    private static SubDomainNameMatch ssm = null;
    private static Lock dnVipLocatorLock = new ReentrantLock();
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(DomainNameMapOperator.class.getName());
    }

    protected void setupPorts() throws Exception {
        setupPort(new Port(Port.INPUT, IN_PORT));
        setupPort(new Port(Port.OUTPUT, OUT_PORT));
        setupPort(new Port(Port.OUTPUT, ERROR_PORT));
    }

    private void initDNGeoLocator() throws Exception {
        try {
            dnGeoLocatorLock.lock();
            if (dnGeoLocator == null) {
                dnGeoLocator = new HashMap<String, String>();
                ResultSet rs = null;
                try {
                    String sql = "select dn,dn from dim_geo";
                    rs = DaoPool.getDao(RuntimeEnv.METADB_CLUSTER).executeQuery(sql);
                    while (rs.next()) {
                        dnGeoLocator.put(rs.getString("dn").toUpperCase(), rs.getString("dn").toUpperCase());
                    }
                } catch (Exception ex) {
                    dnGeoLocator = null;
                    logger.warn(ex.getMessage(), ex);
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
        } finally {
            dnGeoLocatorLock.unlock();
        }
    }

    private void initDNCategoryLocator() throws Exception {
        try {
            dnCategoryLocatorLock.lock();
            if (dnCategoryLocator == null) {
                dnCategoryLocator = new HashMap<String, String>();
                ResultSet rs = null;
                try {
                    String sql = "select id,category_name from dim_dn_category";
                    rs = DaoPool.getDao(RuntimeEnv.METADB_CLUSTER).executeQuery(sql);
                    while (rs.next()) {
                        dnCategoryLocator.put(rs.getString("category_name").toUpperCase(), rs.getString("id"));
                    }
                } catch (Exception ex) {
                    dnCategoryLocator = null;
                    logger.warn(ex.getMessage(), ex);
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
        } finally {
            dnCategoryLocatorLock.unlock();
        }
    }

    private void initDNVipLocator() throws Exception {
        try {
            dnVipLocatorLock.lock();
            if (dnVipLocator == null) {
                dnVipLocator = new HashMap<String, String>();
                ResultSet rs = null;
                String sql = "";
                List<String> domainNameList = new ArrayList<String>();
                List<String> excludeStrList = new ArrayList<String>();

                try {
                    sql = "select domain from dic_vip_domain";
                    rs = DaoPool.getDao(RuntimeEnv.METADB_CLUSTER).executeQuery(sql);
                    while (rs.next()) {
                        domainNameList.add(rs.getString("domain").toUpperCase());
                    }
                } catch (Exception ex) {
                    logger.warn(ex.getMessage(), ex);
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

                try {
                    sql = "select category_name from dim_dn_category";
                    rs = DaoPool.getDao(RuntimeEnv.METADB_CLUSTER).executeQuery(sql);
                    while (rs.next()) {
                        excludeStrList.add(rs.getString("category_name").toUpperCase());
                    }
                } catch (Exception ex) {
                    logger.warn(ex.getMessage(), ex);
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

                try {
                    sql = "select dn from dim_geo";
                    rs = DaoPool.getDao(RuntimeEnv.METADB_CLUSTER).executeQuery(sql);
                    while (rs.next()) {
                        excludeStrList.add(rs.getString("dn").toUpperCase());
                    }
                } catch (Exception ex) {
                    logger.warn(ex.getMessage(), ex);
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

                ssm = new SubDomainNameMatch();
                ssm.init(domainNameList, excludeStrList);

                try {
                    sql = "select domain,vip_id from dic_vip_domain";
                    rs = DaoPool.getDao(RuntimeEnv.METADB_CLUSTER).executeQuery(sql);
                    while (rs.next()) {
                        dnVipLocator.put(rs.getString("domain").toUpperCase(), rs.getString("vip_id"));
                    }
                } catch (Exception ex) {
                    dnVipLocator = null;
                    logger.warn(ex.getMessage(), ex);
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
        } finally {
            dnVipLocatorLock.unlock();
        }
    }

    protected void init0() throws Exception {
        initDNGeoLocator();
        initDNCategoryLocator();
        initDNVipLocator();
    }

    public void validate() throws Exception {
        if (getPort(OUT_PORT).getConnector().size() < 1) {
            throw new Exception("out port with no connectors");
        }
    }

    @Override
	public void start()
   	{
   		// TODO Auto-generated method stub
   		synchronized (this)
   		{
   			notifyAll();
   		}
   	}
    
    protected void execute() {
        try {
            init0();
        } catch (Exception ex) {
            logger.warn(ex.getMessage(), ex);
        }

        try {
            while (true) {
                DataSet dataSet = portSet.get(IN_PORT).getNext();

                if (dataSet.isValid()) {
                    int dataSize = dataSet.size();
                    for (Field2DNMap field2DNMap : field2DNMapSet) {
                        if (field2DNMap.locateMethod.equals("DN2GEO")) {
                            int dataSetFieldNum = dataSet.getFieldNum();
                            dataSet.putFieldName2Idx(field2DNMap.dstFieldName, dataSetFieldNum);
                            if (dnGeoLocator != null) {
                                for (int i = 0; i < dataSize; i++) {
                                    Record record = dataSet.getRecord(i);
                                    Field fieldValue = record.getField(field2DNMap.srcFieldName);
                                    if (fieldValue != null) {
                                        String tmpStr = fieldValue.toString().trim().toUpperCase();
                                        int lastDotPos = tmpStr.lastIndexOf('.');
                                        if (lastDotPos < 0) {
                                            record.appendField(null);
                                        } else {
                                            tmpStr = tmpStr.substring(lastDotPos + 1, tmpStr.length());
                                            record.appendField(new StringField(dnGeoLocator.get(tmpStr)));
                                        }
                                    } else {
                                        record.appendField(null);
                                    }
                                }
                            } else {
                                for (int i = 0; i < dataSize; i++) {
                                    Record record = dataSet.getRecord(i);
                                    record.appendField(null);
                                }
                            }
                        } else if (field2DNMap.locateMethod.equals("DN2CATEGORY")) {
                            int dataSetFieldNum = dataSet.getFieldNum();
                            dataSet.putFieldName2Idx(field2DNMap.dstFieldName, dataSetFieldNum);
                            if (dnCategoryLocator != null) {
                                for (int i = 0; i < dataSize; i++) {
                                    Record record = dataSet.getRecord(i);
                                    Field fieldValue = record.getField(field2DNMap.srcFieldName);

                                    if (fieldValue != null) {
                                        String tmpStr = fieldValue.toString().trim().toUpperCase();
                                        String[] tmpStrItem = tmpStr.split("\\.");
                                        if ((tmpStrItem.length > 0) && (dnCategoryLocator.get(tmpStrItem[tmpStrItem.length - 1]) != null)) {
                                            record.appendField(new StringField(tmpStrItem[tmpStrItem.length - 1]));
                                        } else if ((tmpStrItem.length > 1) && (dnCategoryLocator.get(tmpStrItem[tmpStrItem.length - 2]) != null)) {
                                            record.appendField(new StringField(tmpStrItem[tmpStrItem.length - 2]));
                                        } else {
                                            record.appendField(null);
                                        }
                                    } else {
                                        record.appendField(null);
                                    }
                                }
                            } else {
                                for (int i = 0; i < dataSize; i++) {
                                    Record record = dataSet.getRecord(i);
                                    record.appendField(null);
                                }
                            }
                        } else if (field2DNMap.locateMethod.equals("DN2VIP")) {
                            int dataSetFieldNum = dataSet.getFieldNum();
                            dataSet.putFieldName2Idx(field2DNMap.dstFieldName, dataSetFieldNum);
                            if (dnVipLocator != null) {
                                for (int i = 0; i < dataSize; i++) {
                                    Record record = dataSet.getRecord(i);
                                    //fixme
                                    Field fieldValue = record.getField(field2DNMap.srcFieldName);
                                    if (fieldValue != null) {
                                        String domainName = fieldValue.toString().trim().toUpperCase();
                                        String res = dnVipLocator.get(domainName);
                                        if (res == null || res.equals("")) {
                                            String mainDomainName = ssm.getMainDomain(domainName.toUpperCase());
                                            res = dnVipLocator.get(mainDomainName);
                                        }

                                        if (res != null) {
                                            record.appendField(new StringField(res));
                                        } else {
                                            record.appendField(null);
                                        }
                                    } else {
                                        record.appendField(null);
                                    }
                                }
                            } else {
                                for (int i = 0; i < dataSize; i++) {
                                    Record record = dataSet.getRecord(i);
                                    record.appendField(null);
                                }
                            }
                        } else if (field2DNMap.locateMethod.equals("DN2TLD")) {
                            int dataSetFieldNum = dataSet.getFieldNum();
                            dataSet.putFieldName2Idx(field2DNMap.dstFieldName, dataSetFieldNum);
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                //fixme
                                Field fieldValue = record.getField(field2DNMap.srcFieldName);
                                if (fieldValue != null) {
                                    String tmpStr = fieldValue.toString().trim().toUpperCase();
                                    int lastDotPos = tmpStr.lastIndexOf('.');
                                    if (lastDotPos < 0) {
                                        record.appendField(null);
                                    } else {
                                        tmpStr = tmpStr.substring(lastDotPos + 1, tmpStr.length());
                                        if (!tmpStr.isEmpty()) {
                                            try {
                                                Integer.parseInt(tmpStr);
                                                record.appendField(null);
                                            } catch (Exception ex) {
                                                record.appendField(new StringField("." + tmpStr));
                                            }
                                        } else {
                                            record.appendField(null);
                                        }
                                    }
                                } else {
                                    record.appendField(null);
                                }
                            }
                        } else {
                            //fixme
                        }
                    }
                    portSet.get(OUT_PORT).write(dataSet);
                    reportExecuteStatus();
                } else {
                    portSet.get(OUT_PORT).write(dataSet);
                    break;
                }
            }
            status = SUCCEEDED;
        } catch (Exception ex) {
            ex.printStackTrace();
            status = FAILED;
        } finally {
            try {
                portSet.get(OUT_PORT).write(DataSet.getDataSet(null, DataSet.EOS));
            } catch (Exception ex2) {
                status = FAILED;
                logger.error("Writing DataSet.EOS is failed for" + ex2.getMessage(), ex2);
            }
            reportExecuteStatus();
        }
    }

    @Override
    protected void parseParameters(String pParameters) throws Exception {
        Document document = DocumentHelper.parseText(pParameters);
        Element operatorElt = document.getRootElement();
        Iterator parameterItor = operatorElt.element("parameterlist").elementIterator("parametermap");

        while (parameterItor.hasNext()) {
            Element paraMapElt = (Element) parameterItor.next();
            String srcFieldName = paraMapElt.attributeValue("srcfieldname");
            String dstFieldName = paraMapElt.attributeValue("dstfieldname");
            String locateMethod = paraMapElt.attributeValue("locatemethod");
            field2DNMapSet.add(new Field2DNMap(srcFieldName, dstFieldName, locateMethod));
        }
    }

    class Field2DNMap {

        String srcFieldName;
        String dstFieldName;
        String locateMethod;

        public Field2DNMap(String pSrcFieldName, String pDstFieldName, String pLocateMethod) {
            this.srcFieldName = pSrcFieldName;
            this.dstFieldName = pDstFieldName;
            this.locateMethod = pLocateMethod;
        }
    }

    public void test() {
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

        DomainNameMapOperator dnmOp = new DomainNameMapOperator();
        dnmOp.init0();

    }

	@Override
	public void commit()
	{
		// TODO Auto-generated method stub
		
	}
}
