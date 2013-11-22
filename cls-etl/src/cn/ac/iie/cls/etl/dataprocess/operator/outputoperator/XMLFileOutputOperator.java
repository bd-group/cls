/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator.outputoperator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;

import cn.ac.iie.cls.etl.cc.slave.etltask.ETLTask;
import cn.ac.iie.cls.etl.dataprocess.commons.RuntimeEnv;
import cn.ac.iie.cls.etl.dataprocess.dataset.DataSet;
import cn.ac.iie.cls.etl.dataprocess.dataset.Record;
import cn.ac.iie.cls.etl.dataprocess.operator.Operator;
import cn.ac.iie.cls.etl.dataprocess.operator.Port;
import cn.ac.iie.cls.etl.dataprocess.operator.inputoperator.Column;
import cn.ac.iie.cls.etl.dataprocess.operator.inputoperator.Column.ColumnType;
import cn.ac.iie.cls.etl.dataprocess.util.fs.VFSUtil;

/**
 *
 * @author alexmu
 */
public class XMLFileOutputOperator extends Operator {

	public static final String IN_PORT = "inport1";
    public static final String OUT_PORT = "outport1";
    public static final String ERRDATA_PORT = "error1";

    private String basePath = "";
    private String xmlFileName = "";
    private String xpathForRecord = "";
    private String fileEncoding = "";
    private boolean fileTimestamp = false;
    private String timestampFormat = "";
    private List<Column> columnSet = new ArrayList<Column>();
    private List<String> fieldnameList = new ArrayList<String>();
    //private Map<String,String> xPathMap = new HashMap<String,String>();
    static Logger logger = null;
    
    static {
    	PropertyConfigurator.configure("log4j.properties");
    	logger = Logger.getLogger(XMLFileOutputOperator.class);
    }
    
    protected void setupPorts() throws Exception{
        setupPort(new Port(Port.OUTPUT, OUT_PORT));
        setupPort(new Port(Port.OUTPUT, ERRDATA_PORT));
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
    
    //读取文件
    protected void execute() 
    {
    	try{
    		while (true) {
	    		DataSet dataSet = portSet.get(IN_PORT).getNext();
    			//DataSet dataSet = DataSet.createDataSet();
	    		OutputFormat format = OutputFormat.createPrettyPrint();
	    		format.setEncoding(fileEncoding);
	    		String filePath = xmlFileName;
	    		if(fileTimestamp) {
	    			int dotposi = filePath.lastIndexOf(".");
	    			DateFormat dateformat= new SimpleDateFormat(timestampFormat);
	    			filePath = filePath.substring(0, dotposi) + dateformat.format(new Date()) + ".xml";
	    		}
	    		XMLWriter xmlwriter = new XMLWriter(new FileOutputStream(filePath), format);
	    		if(dataSet.isValid())
	    		{
		    		Document document = DocumentHelper.createDocument();
		    		//定义根节点root
		    		Element root = document.addElement("rows");
		    		
		    		for (int i=0; i<dataSet.size(); i++)
		    		{
		    			//定义根节点ROOT的子节点们
		    			Element sonroot = root.addElement("row");
		    			Record record = dataSet.getRecord(i);
		    			for(int j=0; j<fieldnameList.size(); j++)
		    			{
				    		//定义子节点的子节点
		    				String fieldname = fieldnameList.get(j);
				    		sonroot.addElement(columnSet.get(j).getColumnName()).addText(record.getField(fieldname).toString());
		    			}
		    		}
		    		
		    		xmlwriter.write(document);
		    		xmlwriter.flush();
		    		
		    		logger.info("xmlfile " + filePath + " successfully created");
		    		status = SUCCEEDED;
	    		}
	    		else
	    		{
	    			xmlwriter.close();
                    VFSUtil.putFile(filePath, RuntimeEnv.getParam(RuntimeEnv.HDFS_CONN_STR) + basePath);
	    		}
    		}
    	} catch (Exception e) {
    			status = FAILED;
    			logger.error("Exception occured during of create xmlfile", e);
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
        
        String operatorName = operatorElt.attributeValue("name");
        
        Iterator parameterItor = operatorElt.elementIterator("parameter");
        if(!parameterItor.hasNext()) {
        	status = FAILED;
        	logger.error("operator " + operatorName + ": xml file lacks root element");
        	throw new Exception("operator " + operatorName + ": xml file lacks root element");
        }
        while (parameterItor.hasNext()) {
            Element parameterElement = (Element) parameterItor.next();
            String parameterName = parameterElement.attributeValue("name");
            if(parameterName.equals("basePath")) {
            	basePath = parameterElement.getStringValue().trim();
            	if(basePath.isEmpty()) {
            		logger.warn("operator " + operatorName + ": basePath is null");
                	throw new Exception("operator " + operatorName + ": basePath is null");
            	}
            } else if (parameterName.equals("xmlFile")) {
                xmlFileName = parameterElement.getStringValue().trim();
                if(xmlFileName.isEmpty()) {
                	logger.warn("operator " + operatorName + ": xmlFilePath is null");
                	throw new Exception("operator " + operatorName + ": xmlFilePath is null");
                }
                else if(!xmlFileName.endsWith(".xml")) {
                	logger.warn("operator " + operatorName + ": xmlFilePath is not ends width \".xml\"");
                	throw new Exception("operator " + operatorName + ": xmlFilePath is not ends width \".xml\"");
                }
            } else if (parameterName.equals("fileEncoding")) {
                fileEncoding = parameterElement.getStringValue();
                if(fileEncoding.equals(""))
                {
                	logger.warn("operator " + operatorName + ": fileEncoding is null");
                	throw new Exception("operator " + operatorName + ": fileEncoding is null");
                }
            } else if (parameterName.equals("fileTimestamp")) {
            	fileTimestamp = Boolean.parseBoolean(parameterElement.getStringValue());
            } else if (parameterName.equals("timestampFormat")) {
            	timestampFormat = parameterElement.getStringValue();
            	if(timestampFormat.isEmpty()) {
            		status = FAILED;
            		logger.warn("operator " + operatorName + ": timestampFormat is null");
                	throw new Exception("operator " + operatorName + ": timestampFormat is null");
            	}
            } else {
            	logger.warn("operator " + operatorName + ": Wrong configuration parameters");
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
            	status = FAILED;
            	logger.warn("operator " + operatorName + ": fieldname is null");
            	throw new Exception("operator " + operatorName + ": fieldname is null");
            }
            String columnName = parametermapElt.attributeValue("columnname");
            if(columnName == null)
            {
            	status = FAILED;
            	logger.warn("operator " + operatorName + ": columnname is null");
            	throw new Exception("operator " + operatorName + ": columnname is null");
            }
            ColumnType columnType = Column.parseType(parametermapElt.attributeValue("type"));
            String format =parametermapElt.attributeValue("format");
            if(format != null) {
            	format.trim();
            }
            System.out.println("fornm" + columnType + ", " + format);
            fieldnameList.add(fieldName);
            columnSet.add(new Column(columnName, -1, columnType, format));
        }
        
        Collections.sort(columnSet);
        logger.info("operator " + operatorName + ": Argument parsing is successful");
       
    }
    
    public static void main(String[] args){
    	File inputXml = new File("XMLFileOutputOperator-test-specific.xml");
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
