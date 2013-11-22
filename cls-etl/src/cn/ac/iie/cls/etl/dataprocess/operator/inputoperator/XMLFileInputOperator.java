/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator.inputoperator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import cn.ac.iie.cls.etl.cc.slave.etltask.ETLTask;
import cn.ac.iie.cls.etl.dataprocess.dataset.DataSet;
import cn.ac.iie.cls.etl.dataprocess.dataset.Field;
import cn.ac.iie.cls.etl.dataprocess.dataset.FieldFactory;
import cn.ac.iie.cls.etl.dataprocess.dataset.Record;
import cn.ac.iie.cls.etl.dataprocess.operator.Operator;
import cn.ac.iie.cls.etl.dataprocess.operator.Port;
import cn.ac.iie.cls.etl.dataprocess.operator.inputoperator.Column.ColumnType;
import cn.ac.iie.cls.etl.dataprocess.util.fs.VFSUtil;

/**
 *
 * @author alexmu
 */
public class XMLFileInputOperator extends Operator {

	public static final String IN_PORT = "inport1";
    public static final String OUT_PORT = "outport1";
    public static final String ERRDATA_PORT = "error1";

    private String basePath = "";
    private String xpathForRecord = "";
    private String xmlFilePath="";
    private String fileEncoding="";
    private List<Column> columnSet = new ArrayList<Column>();
    private Map<String,String> xPathMap = new HashMap<String,String>();
    static Logger logger = null;
  //使用一个List嵌套另一个List来读取全部数据
    List<List<Element>> allElemDataList = null;
    
    static {
    	PropertyConfigurator.configure("log4j.properties");
    	logger = Logger.getLogger(XMLFileInputOperator.class);
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
			DataSet dataSet = DataSet.getDataSet(columnSet, DataSet.VALID);
            
            Record record = null;
            for (int i =0; i<allElemDataList.get(0).size(); i++)
            {
               record = new Record();
               for (int j=0; j<allElemDataList.size(); j++) {
            	   record.appendField(FieldFactory.getField(allElemDataList.get(j).get(i).getText(), columnSet.get(j)));
               }
               
               dataSet.appendRecord(record);
               
               if (dataSet.size() >= 1000) {
            	   portSet.get(OUT_PORT).write(dataSet);
            	   reportExecuteStatus();
                   dataSet = DataSet.getDataSet(columnSet, DataSet.VALID);
               }
            }
            if (dataSet.size() > 0) {
            	portSet.get(OUT_PORT).write(dataSet);
            }
            System.out.println("dataSetSize = " + dataSet.size());
            status = SUCCEEDED;
            logger.info("Input xmlfile " + basePath + "/" + xmlFilePath + " is successfully");
    	} catch (Exception ex) {
            status = FAILED;
            logger.error("Input xmlFile is failed for "+ex.getMessage(), ex);             
        } finally {
            try {
                portSet.get(OUT_PORT).write(DataSet.getDataSet(columnSet,DataSet.EOS));
            } catch (Exception ex) {
            	status = FAILED;
                logger.error("Writing DataSet.EOS failed for " + ex.getMessage(), ex);
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
            if (parameterName.equals("basePath")) {
            	basePath = parameterElement.getStringValue();
            	if(basePath.isEmpty()) {
            		logger.warn("operator " + operatorName + ": basePath is null");
                	throw new Exception("operator " + operatorName + ": basePath is null");
            	}
            } else if (parameterName.equals("xpathForRecord")) {
            	xpathForRecord = parameterElement.getStringValue();
                if(xpathForRecord.isEmpty())
                {
                	logger.warn("operator " + operatorName + ": xpathForRecord is null");
                	throw new Exception("operator " + operatorName + ": xpathForRecord is null");
                }
            } else if (parameterName.equals("xmlFile")) {
                xmlFilePath = parameterElement.getStringValue();
                if(xmlFilePath.isEmpty())
                {
                	logger.warn("operator " + operatorName + ": xmlFile is null");
                	throw new Exception("operator " + operatorName + ": xmlFile is null");
                }
            } else if (parameterName.equals("fileEncoding")) {
                fileEncoding = parameterElement.getStringValue();
                if(fileEncoding.equals(""))
                {
                	logger.warn("operator " + operatorName + "," + xmlFilePath + ": fileEncoding is null");
                	throw new Exception("operator " + operatorName + "," + xmlFilePath + ": fileEncoding is null");
                }
            } else {
            	logger.warn("operator " + operatorName + ": Wrong configuration parameters");
            }
        }

        Element parameterListElt = operatorElt.element("parameterlist");
        Iterator parametermapItor = parameterListElt.elementIterator("parametermap");
        if(!parametermapItor.hasNext()) {
        	logger.error("operator " + operatorName + "," + xmlFilePath +  ": xml file lacks parameterlist");
        	throw new Exception("operator " + operatorName + "," + xmlFilePath +  ": xml file lacks parameterlist");
        }
        while (parametermapItor.hasNext()) {
            Element parametermapElt = (Element) parametermapItor.next();
            String columnName = parametermapElt.attributeValue("alias");
            if(columnName == null)
            {
            	status = FAILED;
            	logger.warn("operator " + operatorName + "," + xmlFilePath +  ": alias is null");
            	throw new Exception("operator " + operatorName + "," + xmlFilePath +  ": alias is null");
            }
            ColumnType columnType = Column.parseType(parametermapElt.attributeValue("columntype"));
            String xPath =parametermapElt.attributeValue("xpath");
            if(xPath == null)
            {
            	status = FAILED;
            	logger.warn("operator " + operatorName + "," + xmlFilePath +  ": xPath is null");
            	throw new Exception("operator " + operatorName + "," + xmlFilePath +  ": xPath is null");
            }
            String format =parametermapElt.attributeValue("format");
            xPathMap.put(columnName, xPath);
            columnSet.add(new Column(columnName, -1, columnType, format));
        }
        
        Collections.sort(columnSet);
        logger.info("operator " + operatorName + ": Argument parsing is successful");
        try {
        	File inputFile = VFSUtil.getFile(basePath + "/" + xmlFilePath);
        	System.out.println(inputFile.length());
        	if (!inputFile.isFile()) {
        		throw new Exception("failed to read inputXMLFile " + basePath + "/" + xmlFilePath);
        	}
        	//得到解析器
            SAXReader saxReader = new SAXReader();
            //指定解析文件
            Document xmlDocument =saxReader.read(inputFile);
            allElemDataList = new ArrayList<List<Element>>(); 
            for (int i=0; i<columnSet.size(); i++) {
            	allElemDataList.add(xmlDocument.selectNodes(xPathMap.get(columnSet.get(i).getColumnName())));
            }
        } catch (DocumentException ex) {
            ex.printStackTrace();
            status = FAILED;
            logger.warn("loading document from " + basePath + "/" + xmlFilePath + "is failed for " + ex.getMessage(), ex);
        } catch (Exception e) {
        	logger.error("Reading file is failed for "+e.getMessage(), e);
        }
    }
    
    public static void main(String[] args){
    	File inputXml = new File("XMLFileInputOperator-test-specific.xml");
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
	}
    
    
}
