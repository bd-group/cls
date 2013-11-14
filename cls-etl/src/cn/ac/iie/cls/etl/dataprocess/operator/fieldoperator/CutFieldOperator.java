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
public class CutFieldOperator extends Operator {

    public static final String IN_PORT = "inport1";
    public static final String OUT_PORT = "outport1";
    public static final String ERROR_PORT = "error1";
    List<Field2Cut> field2CutSet = new ArrayList<Field2Cut>();
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(CutFieldOperator.class.getName());
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

    protected void execute() {
        try {
            while (true) {
                DataSet dataSet = portSet.get(IN_PORT).getNext();
                if (dataSet.isValid()) {
                    int dataSize = dataSet.size();
                    for (Field2Cut field2Cut : field2CutSet) {
                        if (field2Cut.useReg) {                           
                            String currentFieldValue = null;
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                currentFieldValue = record.getField(field2Cut.fieldName).toString(); 
                                String[] currentFieldValueStr=currentFieldValue.split(field2Cut.regPattern);
                                for(int j=0;j<currentFieldValueStr.length;j++){
                                    currentFieldValue+=currentFieldValueStr[j];
                                }
                                record.setField(field2Cut.fieldName, new StringField(currentFieldValue));
                            }
                        } else {
                            String currentFieldValue = null;
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                currentFieldValue = record.getField(field2Cut.fieldName).toString();
                                if (field2Cut.startIdx >= field2Cut.endIdx) {
                                    logger.warn("startIdx not less than endIdx plase check the configuration files");
                                    break;
                                }
                                if (field2Cut.startIdx < 0) {
                                    field2Cut.startIdx = 0;
                                }
                                if(currentFieldValue==null){
                                    continue;
                                }
                                if (currentFieldValue.length() >= field2Cut.endIdx) {
                                    record.setField(field2Cut.fieldName, new StringField(currentFieldValue.substring(field2Cut.startIdx, field2Cut.endIdx)));
                                } else {
                                    record.setField(field2Cut.fieldName, new StringField(currentFieldValue.substring(field2Cut.startIdx, currentFieldValue.length())));
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
            logger.info("Cutting  field is successfully");
        } catch (Exception ex) {
            ex.printStackTrace();
            status = FAILED;
            logger.error("Cutting  field is failed for "+ex.getMessage());
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
            boolean useReg = Boolean.parseBoolean(paraMapElt.attributeValue("useregularexpression"));
            String startIdx = paraMapElt.attributeValue("firstcharacterindex");
            String endIdx = paraMapElt.attributeValue("lastcharacterindex");
            String regPattern = paraMapElt.attributeValue("pattern");
            if (useReg) {
                if (regPattern.isEmpty()) {
                    logger.error("useregularexpression is true so pattern connot be null!");
                    throw new Exception("useregularexpression is true so pattern connot be null!");
                }
                field2CutSet.add(new Field2Cut(fieldName, useReg, 0, 0, regPattern));
            } else {
                if (startIdx.isEmpty() || endIdx.isEmpty()) {
                    logger.error("useregularexpression is false so startIdx or endIdx connot be null!");
                    throw new Exception("useregularexpression is false so startIdx or endIdx connot be null!");
                }
                field2CutSet.add(new Field2Cut(fieldName, useReg, Integer.parseInt(startIdx), Integer.parseInt(endIdx), regPattern));
            }

        }
    }

    class Field2Cut {

        String fieldName;
        boolean useReg;
        String regPattern;
        int startIdx;
        int endIdx;

        public Field2Cut(String pFieldName, boolean pUseReg, int pStartIdx, int pEndIdx, String pRegPattern) {
            fieldName = pFieldName;
            useReg = pUseReg;
            regPattern = pRegPattern;
            startIdx = pStartIdx;
            endIdx = pEndIdx;
        }
    }

    public static void main(String[] args) {
        File inputXml = new File("CutFieldOperator-test-specific.xml");
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
