package cn.ac.iie.cls.etl.dataprocess.operator.scriptoperator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

import cn.ac.iie.cls.etl.cc.slave.etltask.ETLTask;
import cn.ac.iie.cls.etl.dataprocess.dataset.DataSet;
import cn.ac.iie.cls.etl.dataprocess.dataset.Field;
import cn.ac.iie.cls.etl.dataprocess.operator.Operator;
import cn.ac.iie.cls.etl.dataprocess.operator.Port;
import cn.ac.iie.cls.etl.dataprocess.operator.inputoperator.Column;
import cn.ac.iie.cls.etl.dataprocess.operator.inputoperator.XMLFileInputOperator;
import cn.ac.iie.cls.etl.dataprocess.operator.inputoperator.Column.ColumnType;

/**
 *
 * @author hanbing
 *
 */
public class JavaScriptOperator extends Operator {

    public static final String IN_PORT = "inport1";
    public static final String OUT_PORT = "outport1";
    public static final String ERRDATA_PORT = "error1";
    private List<Rename> renameSet = new ArrayList<Rename>();
    static Logger logger = null;
    Invocable invoke = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(XMLFileInputOperator.class);
    }

    protected void setupPorts() throws Exception {
        setupPort(new Port(Port.OUTPUT, OUT_PORT));
        setupPort(new Port(Port.OUTPUT, ERRDATA_PORT));
    }

    @Override
    protected void parseParameters(String pParameters) throws Exception {
    	logger.info("start parsing");
        Document document = DocumentHelper.parseText(pParameters);
        Element operatorElt = document.getRootElement();
        
        String javaScriptCode = "";
        //Element operatorElement = operatorElt.element("operator");
        String operatorName = operatorElt.attributeValue("name");

        Iterator parameterItor = operatorElt.elementIterator("parameter");
        if (!parameterItor.hasNext()) {
            logger.error("operator " + operatorName + ": xml file lacks root element");
            throw new Exception("operator " + operatorName + ": xml file lacks root element");
        }
        while (parameterItor.hasNext()) {
            Element parameterElement = (Element) parameterItor.next();
            String parameterName = parameterElement.attributeValue("name");
            if (parameterName.equals("script")) {
                javaScriptCode = parameterElement.getStringValue();
                if (javaScriptCode.isEmpty()) {
                    logger.warn("operator " + operatorName + ": javascript code is null");
                    throw new Exception("operator " + operatorName + ": javascript code is null");
                }
            } else if (parameterName.equals("datasetFlag")) {
               
            } else {
                logger.warn("Wrong configuration parameters");
            }
        }
        
        Element parameterListElt = operatorElt.element("parameterlist");
        Iterator parametermapItor = parameterListElt.elementIterator("parametermap");
        if(!parametermapItor.hasNext()) {
        	logger.error("operator " + operatorName + ": xml file lacks parameterlist");
        	throw new Exception("operator " + operatorName + ": xml file lacks parameterlist");
        }
        while (parametermapItor.hasNext()) {
            Element parametermapElt = (Element) parametermapItor.next();
            String fieldName = parametermapElt.attributeValue("fieldname");
            if(fieldName == null)
            {
            	logger.warn("operator " + operatorName +  ": fieldname is null");
            	throw new Exception("operator " + operatorName +  ": fieldname is null");
            }
            String rename = parametermapElt.attributeValue("columntype");
            String fieldType =parametermapElt.attributeValue("fieldtype");
            int fieldLength = Integer.parseInt(parametermapElt.attributeValue("fieldlength"));
            renameSet.add(new Rename(fieldName, rename, fieldType, fieldLength));
        }
        
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByName("javascript");
        try {
        	engine.eval(javaScriptCode);
        } catch(ScriptException e) {
        	logger.warn("there are errors in the javascript" + e.getMessage(), e);
        	throw new Exception("there are errors in the javascript" + e.getMessage(), e);
        }
        if (engine instanceof Invocable) {
        	invoke = (Invocable) engine;
        }
    }

    @Override
    protected void init0() throws Exception {
    }

    @Override
    public void validate() throws Exception {
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
    
    @Override
    protected void execute() {
        try {
            while (true) {
            	if (null != invoke) {
	                DataSet dataSet = portSet.get(IN_PORT).getNext();
	                if (dataSet.isValid()) {
	                    DataSet newdataSet = (DataSet) invoke.invokeFunction("javascriptFun", dataSet);
	                    System.out.println("output " + newdataSet.size() + " records");
	                    portSet.get(OUT_PORT).write(newdataSet);
	                } else {
	                	portSet.get(OUT_PORT).write(dataSet);
	                    break;
	                }
            	} else {
            		status = FAILED;
            		break;
            	}
            }
            
            status = SUCCEEDED;
            logger.info("javascript execute successfully");
        } catch (NoSuchMethodException ex1) {
            status = FAILED;
            logger.warn("some method used in the javascript is undefined" + ex1.getMessage(), ex1);
        } catch (Exception e) {
            status = FAILED;
            logger.warn("there are errors in executing the javascript" + e.getMessage(), e);
        } finally {
            try {
                portSet.get(OUT_PORT).write(DataSet.getDataSet(null, DataSet.EOS));
            } catch (Exception ex2) {
                status = FAILED;
                logger.error("Writing DataSet.EOS failed for " + ex2.getMessage(), ex2);
            }
            reportExecuteStatus();
        }
    }
    
    class Rename {
    	String fieldName;
    	String rename;
    	String fieldType;
    	int fieldLength;
    	
    	public Rename (String pFieldName, String pRename, String pFieldType, int pFieldLength) {
    		pFieldName = this.fieldName;
    		pRename = this.rename;
    		pFieldLength = this.fieldLength;
    		pFieldType = this.fieldType;
    	}
    }
    
    public static void main(String[] args){
    	File inputXml = new File("JavaScriptOperator-test-specific.xml");
        try {
            String dataProcessDescriptor = "";
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(inputXml)));
            String line = null;
            while ((line = br.readLine()) != null) {
                dataProcessDescriptor += line;
            }
            System.out.println("dataProcessDescriptor :" + dataProcessDescriptor);
            ETLTask etlTask = ETLTask.getETLTask(dataProcessDescriptor);
            Thread etlTaskRunner = new Thread(etlTask);
            etlTaskRunner.start();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        
    }
}
