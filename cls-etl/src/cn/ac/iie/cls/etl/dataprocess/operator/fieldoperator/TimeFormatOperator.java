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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
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
public class TimeFormatOperator extends Operator {

    public static final String IN_PORT = "inport1";
    public static final String OUT_PORT = "outport1";
    public static final String ERROR_PORT = "error1";
    List<Field2TimeFormat> field2TimeFormatSet = new ArrayList<Field2TimeFormat>();
    static Logger logger = null;
    
    static {
    	PropertyConfigurator.configure("log4j.properties");
    	logger = Logger.getLogger(TimeFormatOperator.class);
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
            	//for test
            	//DataSet dataSet = DataSet.createDataSet();
                if (dataSet.isValid()) {
                    int dataSize = dataSet.size();
                    for (Field2TimeFormat field2TimeFormat : field2TimeFormatSet) {
                        if (field2TimeFormat.fromType.equalsIgnoreCase("integer") && field2TimeFormat.toType.equalsIgnoreCase("string")) {
                            SimpleDateFormat toSDF = new SimpleDateFormat(field2TimeFormat.toPattern);
                            Field currentFieldValue = null;
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                currentFieldValue = record.getField(field2TimeFormat.fieldName);
                                record.setField(field2TimeFormat.fieldName, new StringField(toSDF.format(new Date(((LongField)currentFieldValue).getLong()))));
                            }
                        } else if (field2TimeFormat.fromType.equalsIgnoreCase("string") && field2TimeFormat.toType.equalsIgnoreCase("string")) {
                            SimpleDateFormat fromSDF = new SimpleDateFormat(field2TimeFormat.fromPattern);
                            SimpleDateFormat toSDF = new SimpleDateFormat(field2TimeFormat.toPattern);
                            Field currentFieldValue = null;
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                currentFieldValue = record.getField(field2TimeFormat.fieldName);
                                record.setField(field2TimeFormat.fieldName, new StringField(toSDF.format(fromSDF.parse(currentFieldValue.toString()))));
                            }
                        }else if(field2TimeFormat.fromType.equalsIgnoreCase("timestamp") && field2TimeFormat.toType.equalsIgnoreCase("string")){ 
                            SimpleDateFormat toSDF = new SimpleDateFormat(field2TimeFormat.toPattern);
                            Field currentFieldValue = null;
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                currentFieldValue = record.getField(field2TimeFormat.fieldName);
                                record.setField(field2TimeFormat.fieldName, new StringField(toSDF.format(new Date(((LongField)currentFieldValue).getLong()))));
                            }
                            
                        }else {
                        	status = FAILED;
                        	logger.warn("time format failed for can't convert from " + field2TimeFormat.fromType + " to " + field2TimeFormat.toType);                    
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
        	logger.warn("time format is failed for " + ex.getMessage(), ex);
            status = FAILED;
        }finally {
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
            String fieldName = paraMapElt.attributeValue("fieldname");
            if(fieldName == null) {
            	logger.warn("operator IPStandardize: fieldname is null");
            	throw new Exception("operator IPStandardize: fieldname is null");
            }
            String fromType = paraMapElt.attributeValue("fromtype");
            if(fromType == null) {
            	logger.warn("operator IPStandardize: fromType is null");
            	throw new Exception("operator IPStandardize: fromType is null");
            } else {
            	fromType = fromType.toLowerCase();
            }
            String fromPattern = paraMapElt.attributeValue("frompattern");
            if(fromType.equals("string")) {
            	if(fromPattern == null) {
            		logger.warn("operator IPStandardize: fromPattern is null");
                	throw new Exception("operator IPStandardize: fromPattern is null");
            	}
            }
            String toType = paraMapElt.attributeValue("totype");
            if(toType == null) {
            	logger.warn("operator IPStandardize: toType is null");
            	throw new Exception("operator IPStandardize: toType is null");
            } else {
            	toType = toType.toLowerCase();
            }
            String toPattern = paraMapElt.attributeValue("topattern");
            if(toPattern == null) {
            	logger.warn("operator IPStandardize: toPattern is null");
            	throw new Exception("operator IPStandardize: toPattern is null");
            }
            field2TimeFormatSet.add(new Field2TimeFormat(fieldName, fromType, fromPattern, toType, toPattern));
        }
    }

    class Field2TimeFormat {

        String fieldName;
        String fromType;
        String fromPattern;
        String toType;
        String toPattern;

        public Field2TimeFormat(String pFieldName, String pFromType, String pFromPattern, String pToType, String pToPattern) {
            this.fieldName = pFieldName;
            this.fromType = pFromType;
            this.fromPattern = pFromPattern;
            this.toType = pToType;
            this.toPattern = pToPattern;
        }
    }
    
    public static void main(String[] args){
    	File inputXml = new File("TimeFormatOperator-test-specific.xml");
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

	@Override
	public void commit()
	{
		// TODO Auto-generated method stub
		
	}
}
