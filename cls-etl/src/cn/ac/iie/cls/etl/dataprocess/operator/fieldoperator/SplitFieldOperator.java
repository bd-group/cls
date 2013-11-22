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
public class SplitFieldOperator extends Operator {

    public static final String IN_PORT = "inport1";
    public static final String OUT_PORT = "outport1";
    public static final String ERROR_PORT = "error1";
    List<Field2Split> field2SplitSet = new ArrayList<Field2Split>();
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(SplitFieldOperator.class.getName());
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
                    int columnIdx = dataSet.getFieldNum();
                    for (int i = 0; i < dataSize; i++) {
                        Record record = dataSet.getRecord(i);
                        String currentFieldValue = null;
                        for (Field2Split field2Split : field2SplitSet) {
                            currentFieldValue = record.getField(field2Split.fieldName).toString();
                            if (currentFieldValue == null) {
                                continue;
                            }
                            if (field2Split.splitPattern.length() == 1) {
                                char pattern = field2Split.splitPattern.charAt(0);
                                if (pattern == '|' || pattern == '\\' || pattern == '*' || pattern == '\'' //考虑单个字符为正则表达式中转义字符的情况
                                        || pattern == '\"' || pattern == '+' || pattern == '^'
                                        || pattern == '$' || pattern == '?' || pattern == '.') //ASK--- |,\,*,',",+,^,$,?,.
                                {
                                    field2Split.splitPattern = "\\" + field2Split.splitPattern;
                                }
                            }
                            String[] fieldItems = currentFieldValue.split(field2Split.splitPattern);
                            if (fieldItems.length == 1) {
                                continue;
                            }
                            for (int j = 0; j < fieldItems.length; j++) {
                                if (dataSet.fieldNameExist(field2Split.fieldName + "_" + j)) {
                                    continue;
                                }
                                dataSet.putFieldName2Idx(field2Split.fieldName + "_" + j, columnIdx++);
                            }
                        }
                    }
                    for (int i = 0; i < dataSize; i++) {
                        Record record = dataSet.getRecord(i);
                        for (int j = record.size(); j < dataSet.getFieldNameList().size(); j++) {
                            record.appendField(null);
                        }
                    }
                    for (int i = 0; i < dataSize; i++) {
                        Record record = dataSet.getRecord(i);
                        String currentFieldValue = null;
                        for (Field2Split field2Split : field2SplitSet) {
                            currentFieldValue = record.getField(field2Split.fieldName).toString();
                            if (currentFieldValue == null) {
                                continue;
                            }
                            String[] fieldItems = currentFieldValue.split(field2Split.splitPattern);
                            if (fieldItems.length == 1) {
                                continue;
                            }
                            for (int j = 0; j < fieldItems.length; j++) {
                                record.setField(field2Split.fieldName + "_" + j, new StringField(fieldItems[j]));
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
            logger.info("Spliting  field is successfully");
        } catch (Exception ex) {
            ex.printStackTrace();
            status = FAILED;
            logger.info("Spliting  field is failed for " + ex.getMessage());
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
            String splitPattern = paraMapElt.attributeValue("splitpattern");
            if (fieldName.isEmpty() || splitPattern.isEmpty()) {
                logger.error("fieldName or splitPattern is null");
                throw new Exception("fieldName or splitPattern connot be  null");
            }
            field2SplitSet.add(new Field2Split(fieldName, splitPattern));
        }
    }

    class Field2Split {

        String fieldName;
        String splitPattern;

        public Field2Split(String pFieldName, String pSplitPattern) {
            fieldName = pFieldName;
            splitPattern = pSplitPattern;
        }
    }

    public static void main(String[] args) {
        File inputXml = new File("SplitFieldOperator-test-specific.xml");
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
