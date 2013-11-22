/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator.recordoperator;

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
public class RecordSplitOperator extends Operator {

    public static final String IN_PORT = "inport1";
    public static final String OUT_PORT = "outport1";
    public static final String ERROR_PORT = "error1";
    private int splitFieldNum=1;
    private List<Record2Split> record2SplitSet = new ArrayList<Record2Split>();
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(RecordSplitOperator.class.getName());
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
                DataSet dataSet = portSet.get(IN_PORT).getNext();
                
                if (dataSet.isValid()) {
                    int dataSize = dataSet.size();
                    List<Record> tempRecordSet = new ArrayList<Record>();
                    for (int i = 0; i < dataSize; i++) {
                        Record record = dataSet.getRecord(i);
                        String currentFieldValue = null;   
                        splitFieldNum=1;
                        for (Record2Split record2Split : record2SplitSet) {
                            currentFieldValue = record.getField(record2Split.fieldName).toString();
                            if(currentFieldValue==null){
                                continue;
                            }
                            String[] fieldItems = currentFieldValue.split(record2Split.splitPattern);
                            if (fieldItems.length != 1) {
                                for (int j = 0; j < fieldItems.length; j++) {
                                    Record tempRecord = (Record) record.copy(record);
                                    tempRecord.setField(record2Split.fieldName, new StringField(fieldItems[j]));
                                    tempRecordSet.add(tempRecord);
                                    splitFieldNum++;
                                }
                            }
                        }
                        if(splitFieldNum==1){
                            tempRecordSet.add(record);
                        }

                    }
                    for (int i = dataSet.size() - 1; i > -1; i--) {
                        dataSet.removeRecord(i);
                    }
                    for (int i = 0; i < tempRecordSet.size(); i++) {
                        dataSet.appendRecord(tempRecordSet.get(i));
                    }
                    portSet.get(OUT_PORT).write(dataSet);
                    reportExecuteStatus();
                } else {
                    portSet.get(OUT_PORT).write(dataSet);
                    break;
                }
            }
            status = SUCCEEDED;
            logger.info("Spliting record is successfully");
        } catch (Exception ex) {
            status = FAILED;
            ex.printStackTrace();
            logger.error("Spliting record is failed for " + ex.getMessage());
        } finally {
            try {
                portSet.get(OUT_PORT).write(DataSet.getDataSet(null, DataSet.EOS));
            } catch (Exception ex) {
            	status = FAILED;
                logger.error("Writing DataSet.EOS failed for " + ex.getMessage(), ex);
            }
            reportExecuteStatus();
        }
    }

    @Override
    protected void parseParameters(String pParameters) throws Exception {
        logger.info("Start parsing");
        Document document = DocumentHelper.parseText(pParameters);
        Element operatorElt = document.getRootElement();
        Iterator parameterItor = operatorElt.element("parameterlist").elementIterator("parametermap");

        while (parameterItor.hasNext()) {
            Element paraMapElt = (Element) parameterItor.next();
            String fieldName = paraMapElt.attributeValue("fieldname");
            String splitPattern = paraMapElt.attributeValue("splitpattern");
            if (fieldName.isEmpty() || splitPattern.isEmpty()) {
                logger.error("fieldName or splitPattern is null");
                throw new Exception("fieldName or splitPattern connot be null");
            }else if(fieldName.equals("time")){
                logger.error("This field cannot be split for "+fieldName);
                throw new Exception("This field cannot be split,please check the field");
            }
            record2SplitSet.add(new Record2Split(fieldName, splitPattern));
        }
        logger.info("Parsing  configuration file is successful");
    }

    class Record2Split {

        String fieldName;
        String splitPattern;

        public Record2Split(String pFieldName, String pSplitPattern) {
            fieldName = pFieldName;
            splitPattern = pSplitPattern;
        }
    }

    public static void main(String[] args) {
        File inputXml = new File("RecordSplitOperator-test-specific.xml");
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
