/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator.recordoperator;

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
import org.moql.EntityMap;
import org.moql.EntityMapImpl;
import org.moql.Filter;
import org.moql.MoqlException;
import org.moql.service.MoqlUtils;

import cn.ac.iie.cls.etl.cc.slave.etltask.ETLTask;
import cn.ac.iie.cls.etl.dataprocess.dataset.DataSet;
import cn.ac.iie.cls.etl.dataprocess.dataset.Field;
import cn.ac.iie.cls.etl.dataprocess.dataset.FieldFactory;
import cn.ac.iie.cls.etl.dataprocess.dataset.Record;
import cn.ac.iie.cls.etl.dataprocess.dataset.StringField;
import cn.ac.iie.cls.etl.dataprocess.operator.Operator;
import cn.ac.iie.cls.etl.dataprocess.operator.Port;
import cn.ac.iie.cls.etl.dataprocess.operator.inputoperator.Column;

/**
 *
 * @author alexmu, hanbing
 */
public class RecordFilterOperator extends Operator {

    public static final String IN_PORT = "inport1";
    public static final String OUT_PORT = "outport1";
    public static final String ERROR_PORT = "error1";
    private List<OutportFields> outportFields = new ArrayList<OutportFields>();
    private String expression = "";
    private boolean dropDataSet = false;
    private boolean outportAllColumn = false;
    Filter filter = null;
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(RecordFilterOperator.class.getName());
    }

    protected void setupPorts() throws Exception {
        setupPort(new Port(Port.INPUT, IN_PORT));
        setupPort(new Port(Port.OUTPUT, OUT_PORT));
        setupPort(new Port(Port.OUTPUT, ERROR_PORT));
    }

    protected void init0() throws Exception {
    }

    public void validate() throws Exception {
    }

    /*private boolean mapExpression(Record record, String expression, List<String> fieldNameList)
    {
    	List<Field> fieldList = record.getAllFields();
    	EntityMap entityMap = new EntityMapImpl();
    	
    	for (int i=0; i<fieldNameList.size(); i++) {
    		entityMap.putEntity(fieldNameList.get(i), fieldList.get(i).toString());
    	}
    	
        return filter.isMatch(entityMap);
    }*/
    private boolean mapExpression(Record record, String expression, DataSet dataSet)
    {
    	List<Field> fieldList = record.getAllFields();
    	List<String> fieldNameList = dataSet.getFieldNameList();
    	EntityMap entityMap = new EntityMapImpl();
    	
    	for (int i=0; i<record.size(); i++) {
    		entityMap.putEntity(fieldNameList.get(i), fieldList.get(dataSet.getFieldIdx(fieldNameList.get(i))).toString());
    	}
    	
        return filter.isMatch(entityMap);
    }
    
    @Override
	public void commit()
   	{
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
        	filter = MoqlUtils.createFilter(expression);
            while (true) {
                DataSet dataSet = portSet.get(IN_PORT).getNext();
                if (dataSet.isValid())
                {
                    boolean isMapped = false;
                    for (int i = 0; i < dataSet.size(); i++) 
                    {
                        Record record = dataSet.getRecord(i);
                        isMapped = mapExpression(record, expression, dataSet);
                        System.out.println("3---" + isMapped + ", " + i + ", " + dataSet.size() + ", " +dropDataSet + ", " + expression);
                        System.out.println("5---"+ dataSet.getFieldNameList().get(0) + ", "+ dataSet.getFieldNameList().get(1) + ", "+ dataSet.getFieldNameList().get(2) + ", "+ ((StringField)record.getField(0)).getString() + ", " + ((StringField)record.getField(1)).getString());
                        if (isMapped)
                        {
                        	if (dropDataSet) {
                            	portSet.get(ERROR_PORT).write(dataSet);
                            	reportExecuteStatus();
                                logger.warn("dateset dropped for some records are matched with " + expression);
                                break;
                            } else {
                            	logger.info("record is removed because it is matched with " + expression);
                                dataSet.removeRecord(i);
                                i -= 1;
                            }
                        }
                    }
                    
                    if (!dropDataSet) 
                    {
                        if (outportAllColumn) {
                            portSet.get(OUT_PORT).write(dataSet);
                        } else {
                        	List<String> fieldNames = dataSet.getFieldNameList();
                        	for (int i=0; i<outportFields.size(); i++) {
                        		fieldNames.remove(outportFields.get(i).fieldName);
                        	}
                        	
                        	for (int i=0; i<fieldNames.size(); i++) {
                        		dataSet.removeField(fieldNames.get(i));
                        	}
                        	System.out.println("4---" + dataSet.getFieldNameList().size() + dataSet.getRecord(0).getAllFields().size());
                        	portSet.get(OUT_PORT).write(dataSet);
                        }
                    }
                }
                else {
                    portSet.get(OUT_PORT).write(dataSet);
                    break;
                }
            }
            status = SUCCEEDED;
        } catch (MoqlException e) {
        	status = FAILED;
            logger.warn("there are errors in the expression " + expression + ", " + e.getMessage(), e);
		} catch (Exception ex) {
        	status = FAILED;
            logger.warn("there are errors in executing the RecordFilter" + ex.getMessage(), ex);
        }finally {
            try {
                portSet.get(OUT_PORT).write(DataSet.getDataSet(null, DataSet.EOS));
            } catch (Exception ex2) {
            	status = FAILED;
                logger.error("Writing DataSet.EOS failed for " + ex2.getMessage(), ex2);
            }
            reportExecuteStatus();
        }
    }

    @Override
    protected void parseParameters(String pParameters) throws Exception {
        logger.info("Start parsing");
        Document document = DocumentHelper.parseText(pParameters);
        Element operatorElt = document.getRootElement();
        
        //Element operatorElement = operatorElt.element("operator");
        String operatorName = operatorElt.attributeValue("name");
        
        Iterator parameterItor = operatorElt.elementIterator("parameter");
        
        if (!parameterItor.hasNext()) {
            status = FAILED;
            logger.error("operator " + operatorName + ": xml file lacks root element");
            throw new Exception("operator " + operatorName + ": xml file lacks root element");
        }
        
        while (parameterItor.hasNext()) {
            Element parameterElement = (Element) parameterItor.next();
            String parameterName = parameterElement.attributeValue("name");
            if (parameterName.equals("expression")) {
                expression = parameterElement.getStringValue();
                if (expression.isEmpty()) {
                	status = FAILED;
                	logger.warn("operator " + operatorName + ": expression is null");
                    throw new Exception("operator " + operatorName + ": expression code is null");
                }
            } else if (parameterName.equals("dropResultSet")) {
                dropDataSet = Boolean.parseBoolean(parameterElement.getStringValue());
            } else if (parameterName.equals("outportAllColumn")) {
                outportAllColumn = Boolean.parseBoolean(parameterElement.getStringValue());
            } else {
                logger.warn("wrong parameter configuration!");
            }
        }
        if (!outportAllColumn) {
            Element parameterListElt = operatorElt.element("parameterlist");
            Iterator parametermapItor = parameterListElt.elementIterator("parametermap");
            while (parametermapItor.hasNext()) {
                Element parametermapElt = (Element) parametermapItor.next();
                String fieldName = parametermapElt.attributeValue("fieldname");
                if(fieldName == null){
                	status = FAILED;
                	logger.warn("operator " + operatorName + ": fieldname is null");
                    throw new Exception("operator " + operatorName + ": fieldname is null");
                }
                outportFields.add(new OutportFields(fieldName));
            }
        }

        logger.info("Parsing  configuration file is successful");
    }

    class OutportFields {

        public String fieldName;

        public OutportFields(String pFieldName) {
            fieldName = pFieldName;
        }
    }
    
    public static void main(String[] args){
    	File inputXml = new File("RecordFilterOperator-test-specific.xml");
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
