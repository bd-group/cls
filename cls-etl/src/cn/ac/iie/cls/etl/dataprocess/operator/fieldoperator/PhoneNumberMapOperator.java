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
import cn.ac.iie.cls.etl.dataprocess.util.rangesearch.RangeSearch;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
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
public class PhoneNumberMapOperator extends Operator {

    public static final String IN_PORT = "inport1";
    public static final String OUT_PORT = "outport1";
    public static final String ERROR_PORT = "error1";
    List<Field2PNMap> field2PNMapSet = new ArrayList<Field2PNMap>();
    //
    private static Map<String, RangeSearch> pnGeoLocator = null;
    private static Lock pnGeoLocatorLock = new ReentrantLock();
    public static final int PREFIX_LENTH = 3;
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(PhoneNumberMapOperator.class.getName());
    }

    protected void setupPorts() throws Exception {
        setupPort(new Port(Port.INPUT, IN_PORT));
        setupPort(new Port(Port.OUTPUT, OUT_PORT));
        setupPort(new Port(Port.OUTPUT, ERROR_PORT));
    }

    protected void init0() throws Exception {
        try {
            pnGeoLocatorLock.lock();
            if (pnGeoLocator == null) {
                ObjectInputStream in = null;
                try {
                    in = new ObjectInputStream(new BufferedInputStream(new FileInputStream("PNGeoLocator.dat")));
                    long startTime = System.nanoTime();
                    pnGeoLocator = (Map<String, RangeSearch>) in.readObject();
                    long endTime = System.nanoTime();
                    logger.info("init  pnGeoLocator successfully in" + (endTime - startTime) / 1000000 + "ms");
                } catch (Exception ex) {
                    logger.warn("init pnGeoLocator unsuccessfully for " + ex, ex);
                } finally {
                    try {
                        in.close();
                    } catch (Exception ex) {
                    }
                }
            }
        } finally {
            pnGeoLocatorLock.unlock();
        }
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
            while (true) {
                DataSet dataSet = portSet.get(IN_PORT).getNext();

                if (dataSet.isValid()) {
                    int dataSize = dataSet.size();
                    for (Field2PNMap field2PNMap : field2PNMapSet) {
                        if (field2PNMap.locateMethod.equals("PN2DISTRICT")) {
                            int dataSetFieldNum = dataSet.getFieldNum();
                            dataSet.putFieldName2Idx(field2PNMap.dstFieldName, dataSetFieldNum);
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                Field field = record.getField(field2PNMap.srcFieldName);
                                String pnStr = field == null ? "" : field.toString();
                                int pnStrLen = pnStr.length();
                                if (pnStrLen >= 11) {
                                    pnStr = pnStr.substring(pnStrLen - 11, pnStrLen);
                                    RangeSearch range = null;
                                    range = pnGeoLocator.get(pnStr.substring(0, PREFIX_LENTH));
                                    record.appendField(range == null ? null : new StringField(range.getValue(Long.parseLong(pnStr), "district")));
                                } else {
                                    record.appendField(null);
                                }
                            }
                        } else if (field2PNMap.locateMethod.equals("PN2ISP")) {
                            int dataSetFieldNum = dataSet.getFieldNum();
                            dataSet.putFieldName2Idx(field2PNMap.dstFieldName, dataSetFieldNum);
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                Field field = record.getField(field2PNMap.srcFieldName);
                                String pnStr = field == null ? "" : field.toString();
                                int pnStrLen = pnStr.length();
                                if (pnStrLen >= 11) {
                                    pnStr = pnStr.substring(pnStrLen - 11, pnStrLen);
                                    RangeSearch range = null;
                                    range = pnGeoLocator.get(pnStr.substring(0, PREFIX_LENTH));
                                    record.appendField(range == null ? null : new StringField(range.getValue(Long.parseLong(pnStr), "isp")));
                                } else {
                                    record.appendField(null);
                                }
                            }
                        } else if (field2PNMap.locateMethod.equals("PN2PROVINCE")) {
                            int dataSetFieldNum = dataSet.getFieldNum();
                            dataSet.putFieldName2Idx(field2PNMap.dstFieldName, dataSetFieldNum);
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                Field field = record.getField(field2PNMap.srcFieldName);
                                String pnStr = field == null ? "" : field.toString();
                                int pnStrLen = pnStr.length();
                                if (pnStrLen >= 11) {
                                    pnStr = pnStr.substring(pnStrLen - 11, pnStrLen);
                                    RangeSearch range = null;
                                    range = pnGeoLocator.get(pnStr.substring(0, PREFIX_LENTH));
                                    record.appendField(range == null ? null : new StringField(range.getValue(Long.parseLong(pnStr), "province")));
                                } else {
                                    record.appendField(null);
                                }
                            }
                        } else if (field2PNMap.locateMethod.equals("PN2REMARK")) {
                            int dataSetFieldNum = dataSet.getFieldNum();
                            dataSet.putFieldName2Idx(field2PNMap.dstFieldName, dataSetFieldNum);
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                Field field = record.getField(field2PNMap.srcFieldName);
                                String pnStr = field == null ? "" : field.toString();
                                int pnStrLen = pnStr.length();
                                if (pnStrLen >= 11) {
                                    pnStr = pnStr.substring(pnStrLen - 11, pnStrLen);
                                    RangeSearch range = null;
                                    range = pnGeoLocator.get(pnStr.substring(0, PREFIX_LENTH));
                                    record.appendField(range == null ? null : new StringField(range.getValue(Long.parseLong(pnStr), "remark")));
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
            field2PNMapSet.add(new Field2PNMap(srcFieldName, dstFieldName, locateMethod));
        }
    }

    class Field2PNMap {

        String srcFieldName;
        String dstFieldName;
        String locateMethod;

        public Field2PNMap(String pSrcFieldName, String pDstFieldName, String pLocateMethod) {
            this.srcFieldName = pSrcFieldName;
            this.dstFieldName = pDstFieldName;
            this.locateMethod = pLocateMethod;
        }
    }

    public void test(String pPnStr) {
        RangeSearch rs = pnGeoLocator.get(pPnStr.substring(0, PREFIX_LENTH));
        System.out.println(rs.getValue(Long.parseLong(pPnStr), "province"));
        System.out.println(rs.getValue(Long.parseLong(pPnStr), "remark"));
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

        PhoneNumberMapOperator pn = new PhoneNumberMapOperator();
        pn.init0();
        pn.test("13161023289");
    }

	@Override
	public void commit()
	{
		// TODO Auto-generated method stub
		
	}
}
