/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator.fieldoperator;

import cn.ac.iie.cls.etl.cc.slave.etltask.ETLTask;
import cn.ac.iie.cls.etl.dataprocess.dataset.BooleanField;
import cn.ac.iie.cls.etl.dataprocess.dataset.DataSet;
import cn.ac.iie.cls.etl.dataprocess.dataset.DoubleField;
import cn.ac.iie.cls.etl.dataprocess.dataset.LongField;
import cn.ac.iie.cls.etl.dataprocess.dataset.MACField;
import cn.ac.iie.cls.etl.dataprocess.dataset.Record;
import cn.ac.iie.cls.etl.dataprocess.dataset.StringField;
import cn.ac.iie.cls.etl.dataprocess.dataset.TimestampField;
import cn.ac.iie.cls.etl.dataprocess.operator.Operator;
import cn.ac.iie.cls.etl.dataprocess.operator.Port;
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
public class AddFieldOperator extends Operator {

    public static final String IN_PORT = "inport1";
    public static final String OUT_PORT = "outport1";
    public static final String ERROR_PORT = "error1";
    private List<Field2Add> field2AddSet = new ArrayList<Field2Add>();
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(AddFieldOperator.class.getName());
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
                if (dataSet.isValid()) {
                    int dataSize = dataSet.size();
                    int dataSetFieldNum = dataSet.getFieldNum();
                    int idx = 0;
                    for (Field2Add columnValueHolder : field2AddSet) {
                        dataSet.putFieldName2Idx(columnValueHolder.fieldName, dataSetFieldNum + (idx++));
                    }
                    for (int i = 0; i < dataSize; i++) {
                        Record record = dataSet.getRecord(i);
                        for (Field2Add columnValueHolder : field2AddSet) {
                            if (columnValueHolder.fieldValue.startsWith("@{")) {
                                String[] currentFieldValueStr = columnValueHolder.fieldValue.split("\\@\\{");
                                String currentFieldValue = currentFieldValueStr[1].substring(0, currentFieldValueStr[1].length() - 1);                                
                                record.appendField(new StringField(record.getField(currentFieldValue).toString()));
                            } else {
                                if(columnValueHolder.fieldType.equalsIgnoreCase("string")){
                                    record.appendField(new StringField(columnValueHolder.fieldValue));
                                }else if(columnValueHolder.fieldType.equalsIgnoreCase("long")){                                    
                                    record.appendField(new LongField(Long.parseLong(columnValueHolder.fieldValue)));
                                }else if(columnValueHolder.fieldType.equalsIgnoreCase("double")){
                                    record.appendField(new DoubleField(Double.parseDouble(columnValueHolder.fieldValue)));
                                }else if(columnValueHolder.fieldType.equalsIgnoreCase("boolean")){
                                    record.appendField(new BooleanField(Boolean.parseBoolean(columnValueHolder.fieldValue)));
                                }else if(columnValueHolder.fieldType.equalsIgnoreCase("timestamp")){
                                    record.appendField(new TimestampField(columnValueHolder.fieldValue));
                                }else if(columnValueHolder.fieldType.equalsIgnoreCase("ip")){
                                    record.appendField(new StringField(columnValueHolder.fieldValue));
                                }else if(columnValueHolder.fieldType.equalsIgnoreCase("mac")){
                                    record.appendField(new MACField(columnValueHolder.fieldValue));
                                }                   
                            }
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
            logger.info("Adding Field is successfully");
        } catch (Exception ex) {
            ex.printStackTrace();
            status = FAILED;
            logger.error("Adding field is failed for " + ex.getMessage());
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
            String fieldName = paraMapElt.attributeValue("fieldname");
            String fieldValue = paraMapElt.attributeValue("fieldvalue");
            String fieldType=paraMapElt.attributeValue("fieldtype");
            if (fieldName.isEmpty() || fieldValue.isEmpty()||fieldType.isEmpty()) {
                logger.error("fieldName or fieldValue is null");
                throw new Exception("fieldName or fieldValue connot be null");
            }
            field2AddSet.add(new Field2Add(fieldName, fieldValue,fieldType));
        }
    }

    class Field2Add {

        String fieldName;
        String fieldValue;
        String fieldType;

        public Field2Add(String pColumnName, String pColumnValue,String pColumnType) {
            fieldName = pColumnName;
            fieldValue = pColumnValue;
            fieldType=pColumnType;
        }
    }

    public static void main(String[] args) {
        File inputXml = new File("AddFieldOperator-test-specific.xml");
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
