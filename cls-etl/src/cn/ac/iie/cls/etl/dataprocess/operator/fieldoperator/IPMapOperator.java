/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator.fieldoperator;

import cn.ac.iie.cls.etl.dataprocess.commons.RuntimeEnv;
import cn.ac.iie.cls.etl.dataprocess.config.Configuration;
import cn.ac.iie.cls.etl.dataprocess.dataset.DataSet;
import cn.ac.iie.cls.etl.dataprocess.dataset.Field;
import cn.ac.iie.cls.etl.dataprocess.dataset.Record;
import cn.ac.iie.cls.etl.dataprocess.dataset.StringField;
import cn.ac.iie.cls.etl.dataprocess.operator.Operator;
import cn.ac.iie.cls.etl.dataprocess.operator.Port;
import cn.ac.iie.cls.etl.dataprocess.util.ip.IPUtil;
import cn.ac.iie.cls.etl.dataprocess.util.rangesearch.RangeSearch;
import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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
public class IPMapOperator extends Operator {
    
    public static final String IN_PORT = "inport1";
    public static final String OUT_PORT = "outport1";
    public static final String ERROR_PORT = "error1";
    List<Field2IPMap> field2IPMapSet = new ArrayList<Field2IPMap>();
    //
    private static RangeSearch ipGeoLocator = null;
    private static Lock ipGeoLocatorLock = new ReentrantLock();
    private static RangeSearch ipVipLocator = null;
    private static Lock ipVipLocatorLock = new ReentrantLock();
    private static LookupService ipLLLocator = null;
    private static Lock ipLLLocatorLock = new ReentrantLock();
    static Logger logger = null;
    
    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(IPMapOperator.class.getName());
    }
    
    protected void setupPorts() throws Exception {
        setupPort(new Port(Port.INPUT, IN_PORT));
        setupPort(new Port(Port.OUTPUT, OUT_PORT));
        setupPort(new Port(Port.OUTPUT, ERROR_PORT));
    }
    
    private void initIPGeoLocator() throws Exception {
        try {
            ipGeoLocatorLock.lock();
            if (ipGeoLocator == null) {
                ObjectInputStream in = null;
                try {
                    in = new ObjectInputStream(new BufferedInputStream(new FileInputStream("IPGeoLocator.dat")));
                    long startTime = System.nanoTime();
                    ipGeoLocator = (RangeSearch) in.readObject();
                    long endTime = System.nanoTime();
                    logger.info("init  ipGeoLocator successfully in" + (endTime - startTime) / 1000000 + "ms");
                } catch (Exception ex) {
                    logger.warn("init ipGeoLocator unsuccessfully for " + ex, ex);
                } finally {
                    try {
                        in.close();
                    } catch (Exception ex) {
                    }
                }
            }
        } finally {
            ipGeoLocatorLock.unlock();
        }
    }
    
    private void initIPVipLocator() throws Exception {
        try {
            ipVipLocatorLock.lock();
            if (ipVipLocator == null) {
                ObjectInputStream in = null;
                try {
                    in = new ObjectInputStream(new BufferedInputStream(new FileInputStream("IPVipLocator.dat")));
                    long startTime = System.nanoTime();
                    ipVipLocator = (RangeSearch) in.readObject();
                    long endTime = System.nanoTime();
                    logger.info("init  ipVipLocator successfully in" + (endTime - startTime) / 1000000 + "ms");
                } catch (Exception ex) {
                    logger.warn("init ipVipLocator unsuccessfully for " + ex, ex);
                } finally {
                    try {
                        in.close();
                    } catch (Exception ex) {
                    }
                }
            }
        } finally {
            ipVipLocatorLock.unlock();
        }
    }
    
    private void initIPLLLocator() throws Exception {
        try {
            ipLLLocatorLock.lock();
            if (ipLLLocator == null) {
                ipLLLocator = new LookupService("GeoLiteCity.dat", LookupService.GEOIP_MEMORY_CACHE);
            }
        } catch (Exception ex) {
            logger.warn("init ipLLLocator unsuccessfully for " + ex.getMessage(), ex);
            ipLLLocator = null;
        } finally {
            ipLLLocatorLock.unlock();
        }
    }
    
    protected void init0() throws Exception {
        initIPGeoLocator();
        initIPVipLocator();
        initIPLLLocator();
    }
    
    public void validate() throws Exception {
        if (getPort(OUT_PORT).getConnector().size() < 1) {
            throw new Exception("out port with no connectors");
        }
    }
    
    protected void execute() {
        try {
            while (true) {
                DataSet dataSet = portSet.get(IN_PORT).getNext();
                
                if (dataSet.isValid()) {
                    int dataSize = dataSet.size();
                    for (Field2IPMap field2IPMap : field2IPMapSet) {
                        if (field2IPMap.locateMethod.equals("IP2COUNTRY")) {
                            int dataSetFieldNum = dataSet.getFieldNum();
                            dataSet.putFieldName2Idx(field2IPMap.dstFieldName, dataSetFieldNum);
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                Field field = record.getField(field2IPMap.srcFieldName);
                                long ipLongValue = field2IPMap.srcFieldType.equals("String") ? IPUtil.IPV4Str2Long(field == null ? null : field.toString()) : Long.parseLong(field == null ? null : field.toString());
                                String country = ipGeoLocator.getValue(ipLongValue, "country");
                                record.appendField(country == null ? null : new StringField(country));
                            }
                        } else if (field2IPMap.locateMethod.equals("IP2DISTRICT")) {
                            int dataSetFieldNum = dataSet.getFieldNum();
                            dataSet.putFieldName2Idx(field2IPMap.dstFieldName, dataSetFieldNum);
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                Field field = record.getField(field2IPMap.srcFieldName);
                                long ipLongValue = field2IPMap.srcFieldType.equals("String") ? IPUtil.IPV4Str2Long(field == null ? null : field.toString()) : Long.parseLong(field == null ? null : field.toString());
                                String district = ipGeoLocator.getValue(ipLongValue, "district");
                                record.appendField(district == null ? null : new StringField(district));
                            }
                        } else if (field2IPMap.locateMethod.equals("IP2ISP")) {
                            int dataSetFieldNum = dataSet.getFieldNum();
                            dataSet.putFieldName2Idx(field2IPMap.dstFieldName, dataSetFieldNum);
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                Field field = record.getField(field2IPMap.srcFieldName);
                                long ipLongValue = field2IPMap.srcFieldType.equals("String") ? IPUtil.IPV4Str2Long(field == null ? null : field.toString()) : Long.parseLong(field == null ? null : field.toString());
                                String isp = ipGeoLocator.getValue(ipLongValue, "isp");
                                record.appendField(isp == null ? null : new StringField(isp));
                            }
                        } else if (field2IPMap.locateMethod.equals("IP2LONTITUDE")) {
                            int dataSetFieldNum = dataSet.getFieldNum();
                            dataSet.putFieldName2Idx(field2IPMap.dstFieldName, dataSetFieldNum);
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                Field field = record.getField(field2IPMap.srcFieldName);
                                long ipLongValue = field2IPMap.srcFieldType.equals("String") ? IPUtil.IPV4Str2Long(field == null ? null : field.toString()) : Long.parseLong(field == null ? null : field.toString());
                                Location location = ipLLLocator.getLocation(ipLongValue);
                                record.appendField(location == null ? null : new StringField(String.valueOf(location.longitude)));
                            }
                        } else if (field2IPMap.locateMethod.equals("IP2LATITUDE")) {
                            int dataSetFieldNum = dataSet.getFieldNum();
                            dataSet.putFieldName2Idx(field2IPMap.dstFieldName, dataSetFieldNum);
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                Field field = record.getField(field2IPMap.srcFieldName);
                                long ipLongValue = field2IPMap.srcFieldType.equals("String") ? IPUtil.IPV4Str2Long(field == null ? null : field.toString()) : Long.parseLong(field == null ? null : field.toString());
                                Location location = ipLLLocator.getLocation(ipLongValue);
                                record.appendField(location == null ? null : new StringField(String.valueOf(location.latitude)));
                            }
                        } else if (field2IPMap.locateMethod.equals("IP2VIP")) {
                            int dataSetFieldNum = dataSet.getFieldNum();
                            dataSet.putFieldName2Idx(field2IPMap.dstFieldName, dataSetFieldNum);
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                Field field = record.getField(field2IPMap.srcFieldName);
                                long ipLongValue = field2IPMap.srcFieldType.equals("String") ? IPUtil.IPV4Str2Long(field == null ? null : field.toString()) : Long.parseLong(field == null ? null : field.toString());
                                List<String> vipidLst = ipVipLocator.getValues(ipLongValue);
                                String vipidsStr = ",";
                                if (vipidLst != null) {
                                    for (String vipid : vipidLst) {
                                        vipidsStr += vipid + ",";
                                    }
                                }
                                record.appendField(vipidsStr.equals(",") ? null : new StringField(vipidsStr));
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
            logger.error("error happened when doing ip mapping " + ex.getMessage(), ex);
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
            String srcFieldName = paraMapElt.attributeValue("srcfieldname").trim();
            String srcFieldType = paraMapElt.attributeValue("srcfieldtype").trim();
            String dstFieldName = paraMapElt.attributeValue("dstfieldname").trim();
            String locateMethod = paraMapElt.attributeValue("locatemethod").trim();
            field2IPMapSet.add(new Field2IPMap(srcFieldName, srcFieldType, dstFieldName, locateMethod));
        }
    }
    
    class Field2IPMap {
        
        String srcFieldName;
        String srcFieldType;
        String dstFieldName;
        String locateMethod;
        
        public Field2IPMap(String pSrcFieldName, String pSrcFieldType, String pDstFieldName, String pLocateMethod) {
            srcFieldName = pSrcFieldName;
            srcFieldType = pSrcFieldType;
            dstFieldName = pDstFieldName;
            locateMethod = pLocateMethod;
        }
    }
    
    public void test(long pIPN) {
        System.out.println(ipGeoLocator.getValue(pIPN, "country"));
        System.out.println(ipGeoLocator.getValue(pIPN, "district"));
        System.out.println(ipGeoLocator.getValue(pIPN, "isp"));
        List<String> vipidLst = ipVipLocator.getValues(pIPN);
        
        String vipidsStr = ",";
        if (vipidLst != null) {
            for (String vipid : vipidLst) {
                vipidsStr += vipid + ",";
            }
        }
        System.out.println(vipidsStr);
        Location location = ipLLLocator.getLocation(pIPN);
        System.out.println(location.latitude + "," + location.longitude);
        
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
        
        IPMapOperator ipMapOperator = new IPMapOperator();
        ipMapOperator.init0();
        ipMapOperator.test(2017968812L);
    }
}
