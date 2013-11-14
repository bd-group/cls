/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator.fieldoperator;

import cn.ac.iie.cls.etl.cc.slave.etltask.ETLTask;
import cn.ac.iie.cls.etl.dataprocess.dataset.DataSet;
import cn.ac.iie.cls.etl.dataprocess.dataset.Field;
import cn.ac.iie.cls.etl.dataprocess.dataset.LongField;
import cn.ac.iie.cls.etl.dataprocess.dataset.Record;
import cn.ac.iie.cls.etl.dataprocess.dataset.StringField;
import cn.ac.iie.cls.etl.dataprocess.operator.Operator;
import cn.ac.iie.cls.etl.dataprocess.operator.Port;
import cn.ac.iie.cls.etl.dataprocess.operator.inputoperator.XMLFileInputOperator;
import cn.ac.iie.cls.etl.dataprocess.util.ip.IPUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

/**
 *
 * @author alexmu
 */
public class IPStandardizeOperator extends Operator {

    public static final String IN_PORT = "inport1";
    public static final String OUT_PORT = "outport1";
    public static final String ERROR_PORT = "error1";
    List<Field2IPStd> field2IPStdSet = new ArrayList<Field2IPStd>();
    static Logger logger = null;
    
    static {
    	PropertyConfigurator.configure("log4j.properties");
    	logger = Logger.getLogger(IPStandardizeOperator.class);
    }
    
    protected void setupPorts() throws Exception {
        setupPort(new Port(Port.INPUT, IN_PORT));
        setupPort(new Port(Port.OUTPUT, OUT_PORT));
        setupPort(new Port(Port.OUTPUT, ERROR_PORT));
    }

    protected void init0() throws Exception {
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
            	//DataSet dataSet = DataSet.createDataSet();
                if (dataSet.isValid()) {
                    int dataSize = dataSet.size();
                    for (Field2IPStd field2IPStd : field2IPStdSet) {
                        if (field2IPStd.fromPattern.equalsIgnoreCase("long") && field2IPStd.toPattern.equalsIgnoreCase("string")) {
                            //fixme
                            Field currentFieldValue = null;
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                currentFieldValue = record.getField(field2IPStd.fieldName);
                                if (currentFieldValue instanceof LongField) {
                                    record.setField(field2IPStd.fieldName, new StringField(IPUtil.IPV4Long2Str(currentFieldValue.toString())));
                                }
                            }
                        } else if (field2IPStd.fromPattern.equalsIgnoreCase("string") && field2IPStd.toPattern.equalsIgnoreCase("long")) {
                            Field currentFieldValue = null;
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                currentFieldValue = record.getField(field2IPStd.fieldName);
                                record.setField(field2IPStd.fieldName, new LongField(IPUtil.IPV4Str2Long(currentFieldValue.toString())));
                            }
                        } else {
                        	status = FAILED;
                        	logger.warn("ip standardize failed for can't convert from fromPattern or toPattern");
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
        	logger.warn("ip standardize is failed for " + ex.getMessage(), ex);
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
            String fieldName = paraMapElt.attributeValue("fieldname").trim();
            if(fieldName == null) {
            	logger.warn("operator IPStandardize: fieldname is null");
            	throw new Exception("operator xml IPStandardize: fieldname is null");
            }
            String type = paraMapElt.attributeValue("type").trim();
            if(type == null) {
            	logger.warn("operator IPStandardize: type is null");
            	throw new Exception("operator xml IPStandardize: type is null");
            }
            String fromPattern = paraMapElt.attributeValue("frompattern").trim();
            if(fromPattern == null) {
            	logger.warn("operator IPStandardize: frompattern is null");
            	throw new Exception("operator xml IPStandardize: frompattern is null");
            }
            String toPattern = paraMapElt.attributeValue("topattern").trim();
            if(toPattern == null) {
            	logger.warn("operator IPStandardize: topattern is null");
            	throw new Exception("operator xml IPStandardize: topattern is null");
            } else {
            	toPattern = toPattern.toLowerCase();
            }
            field2IPStdSet.add(new Field2IPStd(fieldName, type, fromPattern, toPattern));
        }
    }

    class Field2IPStd {

        String fieldName;
        String type;
        String fromPattern;
        String toPattern;

        public Field2IPStd(String pFieldName, String pType, String pFromPattern, String pToPattern) {
            fieldName = pFieldName;
            type = pType;
            fromPattern = pFromPattern;
            toPattern = pToPattern;
        }
    }
    
    public static void main(String[] args){
    	File inputXml = new File("IPStandardizeOperator-test-specific.xml");
        try {
            String dataProcessDescriptor = "";
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(inputXml)));
            String line = null;
            while ((line = br.readLine()) != null) {
                dataProcessDescriptor += line;
            }
            ETLTask etlTask = ETLTask.getETLTask(dataProcessDescriptor);
            Thread etlTaskRunner = new Thread(etlTask);
            etlTaskRunner.start();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        
    }
}
