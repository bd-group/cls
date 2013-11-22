package cn.ac.iie.cls.etl.dataprocess.operator.testoperator;

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
import cn.ac.iie.cls.etl.dataprocess.operator.Operator;
import cn.ac.iie.cls.etl.dataprocess.operator.Port;
import cn.ac.iie.cls.etl.dataprocess.operator.inputoperator.XMLFileInputOperator;

/**
 *
 * @author hanbing
 *
 */
public class Test3Operator extends Operator {

    public static final String IN_PORT = "inport1";
    public static final String OUT_PORT = "outport1";
    public static final String ERRDATA_PORT = "error1";
    static Logger logger = null;
    Invocable invoke = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(XMLFileInputOperator.class);
    }

    protected void setupPorts() throws Exception {
        setupPort(new Port(Port.INPUT, IN_PORT));
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
			//Thread.sleep(500000);
	    	while (true) {
	            DataSet dataSet = portSet.get(IN_PORT).getNext();
	            if (dataSet.isValid())
	            {
	            } else {
	                portSet.get(OUT_PORT).write(dataSet);
	                break;
	            }
	        }
	        status = SUCCEEDED;
	    } catch (Exception ex) {
	    	status = FAILED;
	    } finally {
	        try {
	            portSet.get(OUT_PORT).write(DataSet.getDataSet(null, DataSet.EOS));
	        } catch (Exception ex2) {
	        	status = FAILED;
	        }
	        reportExecuteStatus();
	        System.out.println("test3 exit successfully!!!!!!!!!");
	    }
    }
}
