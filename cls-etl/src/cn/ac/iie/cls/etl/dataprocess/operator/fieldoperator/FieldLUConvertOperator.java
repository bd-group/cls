/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator.fieldoperator;

import cn.ac.iie.cls.etl.cc.slave.etltask.ETLTask;
import cn.ac.iie.cls.etl.dataprocess.dataset.DataSet;
import cn.ac.iie.cls.etl.dataprocess.dataset.Record;
import cn.ac.iie.cls.etl.dataprocess.dataset.StringField;
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
public class FieldLUConvertOperator extends Operator {

    public static final String IN_PORT = "inport1";
    public static final String OUT_PORT = "outport1";
    public static final String ERROR_PORT = "error1";
    List<Field2LUConvert> field2LUConvertSet = new ArrayList<Field2LUConvert>();
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(FieldLUConvertOperator.class.getName());
    }

    @Override
    protected void setupPorts() throws Exception {
        setupPort(new Port(Port.INPUT, IN_PORT));
        setupPort(new Port(Port.OUTPUT, OUT_PORT));
        setupPort(new Port(Port.OUTPUT, ERROR_PORT));
    }

    @Override
    protected void init0() throws Exception {
    }

    @Override
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
    
    @Override
    protected void execute() {
        try {
            while (true) {
                DataSet dataSet = portSet.get(IN_PORT).getNext();
                if (dataSet.isValid()) {
                    int dataSize = dataSet.size();
                    for (Field2LUConvert field2LUConvert : field2LUConvertSet) {
                        String currentFieldValue = "";
                        if (field2LUConvert.convertType.equals("lower")) {
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                currentFieldValue = record.getField(field2LUConvert.fieldName).toString();
                                if (currentFieldValue == null) {
                                    continue;
                                }
                                record.setField(field2LUConvert.fieldName, new StringField(currentFieldValue.toLowerCase()));
                            }
                        } else if (field2LUConvert.convertType.equals("upper")) {
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                currentFieldValue = record.getField(field2LUConvert.fieldName).toString();
                                if (currentFieldValue == null) {
                                    continue;
                                }
                                record.setField(field2LUConvert.fieldName, new StringField(currentFieldValue.toUpperCase()));
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
            logger.info("LUConverting  file is successfully");
        } catch (Exception ex) {
            ex.printStackTrace();
            status = FAILED;
            logger.error("LUConverting  file is failed for "+ex.getMessage());
            
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
            String convertType = paraMapElt.attributeValue("converttype");
            if (fieldName.isEmpty() || convertType.isEmpty()) {
                logger.error("fieldName or convertType connot be null");
                throw new Exception("fieldName or convertType connot be null");
            }
            if (!convertType.equalsIgnoreCase("lower") && !convertType.equalsIgnoreCase("upper")) {
                logger.error("convertType must be lower or upper but the convertType is " + convertType);
                throw new Exception("convertType must be lower or upper but the convertType is " + convertType);
            }
            field2LUConvertSet.add(new Field2LUConvert(fieldName, convertType));
        }
    }

    class Field2LUConvert {

        String fieldName;
        String convertType;

        public Field2LUConvert(String pFieldName, String pConvertType) {
            fieldName = pFieldName;
            convertType = pConvertType;
        }
    }

    public static void main(String[] args) {
        File inputXml = new File("FieldLUConvertOperator-test-specific.xml");
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
