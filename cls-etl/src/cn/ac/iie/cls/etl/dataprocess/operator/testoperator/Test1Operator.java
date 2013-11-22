package cn.ac.iie.cls.etl.dataprocess.operator.testoperator;

import javax.script.Invocable;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

import cn.ac.iie.cls.etl.dataprocess.dataset.DataSet;
import cn.ac.iie.cls.etl.dataprocess.operator.Operator;
import cn.ac.iie.cls.etl.dataprocess.operator.Port;
import cn.ac.iie.cls.etl.dataprocess.operator.inputoperator.XMLFileInputOperator;

/**
 *
 * @author hanbing
 *
 */
public class Test1Operator extends Operator {

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
			Thread.sleep(3000);
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
                //status = FAILED;
	    } catch (Exception ex) {
	    	status = FAILED;
	    } finally {
	        try {
	            portSet.get(OUT_PORT).write(DataSet.getDataSet(null, DataSet.EOS));
	        } catch (Exception ex2) {
	        	status = FAILED;
	        }
	        reportExecuteStatus();
	        System.out.println("test1 exit successfully!!!!!!!!!");
	    }
    }
}
