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
public class FieldMapOperator extends Operator {

    public static final String IN_PORT = "inport1";
    public static final String OUT_PORT = "outport1";
    public static final String ERROR_PORT = "error1";
    List<Field2Map> field2MapSet = new ArrayList<Field2Map>();
    List<Field2MapValue> field2MapValueSet = new ArrayList<Field2MapValue>();
    private String mappingType = "";
    private String defaultValue = "";
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(FieldMapOperator.class.getName());
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
    protected void execute() {
        try {
            while (true) {
                DataSet dataSet = portSet.get(IN_PORT).getNext();
                if (dataSet.isValid()) {
                    int dataSize = dataSet.size();
                    boolean isMapping = false;
                    for (int i = 0; i < dataSize; i++) {
                        Record record = dataSet.getRecord(i);
                        String currentFieldValue = "";
                        for (Field2Map field2Map : field2MapSet) {
                            isMapping = false;
                            currentFieldValue = record.getField(field2Map.fieldName).toString();
                            if (currentFieldValue == null) {
                                continue;
                            }
                            if (mappingType.equals("exact")) {
                                for (Field2MapValue field2MapValue : field2MapValueSet) {
                                    if (currentFieldValue.equals(field2MapValue.fromValue)) {
                                        record.setField(field2Map.fieldName, new StringField(field2MapValue.toValue));
                                        isMapping = true;
                                        break;
                                    }
                                }
                                if (!defaultValue.isEmpty() && !isMapping) {
                                    record.setField(field2Map.fieldName, new StringField(defaultValue));
                                }
                            } else if (mappingType.equals("pattern")) {
                                for (Field2MapValue field2MapValue : field2MapValueSet) {
                                    record.setField(field2Map.fieldName, new StringField(currentFieldValue.replaceAll(field2MapValue.fromValue, field2MapValue.toValue)));
                                    isMapping = true;
                                    break;
                                }
                                if (!defaultValue.isEmpty() && !isMapping) {
                                    record.setField(field2Map.fieldName, new StringField(defaultValue));
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
            logger.info("Mapping  field is successfully");
        } catch (Exception ex) {
            ex.printStackTrace();
            status = FAILED;
            logger.info("Mapping  field is failed for " + ex.getMessage());
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
        Iterator parameterListItor = operatorElt.elementIterator("parameterList");
        while (parameterListItor.hasNext()) {
            Element parameterlistElt = (Element) parameterListItor.next();
            if (parameterlistElt.attributeValue("name").equals("fields")) {
                Iterator fieldsItor = parameterlistElt.elementIterator("parametermap");
                while (fieldsItor.hasNext()) {
                    Element fieldsElt = (Element) fieldsItor.next();
                    String fieldName = fieldsElt.attributeValue("fieldname");
                    field2MapSet.add(new Field2Map(fieldName));
                }
            } else if (parameterlistElt.attributeValue("name").equals("valueMappings")) {
                Iterator fieldsItor = parameterlistElt.elementIterator("parametermap");
                while (fieldsItor.hasNext()) {
                    Element fieldElt = (Element) fieldsItor.next();
                    String fromvalue = fieldElt.attributeValue("fromvalue");
                    if (fromvalue.isEmpty()) {
                        logger.error("fromvalue is null");
                        throw new Exception("fromvalue connot be null!");
                    }
                    String tovalue = fieldElt.attributeValue("tovalue");
                    field2MapValueSet.add(new Field2MapValue(fromvalue, tovalue));
                }
            }
        }
        Iterator parameterItor = operatorElt.elementIterator("parameter");
        while (parameterItor.hasNext()) {
            Element parameterElt = (Element) parameterItor.next();
            String parameterName = parameterElt.attributeValue("name");
            if (parameterName.equals("mappingType")) {
                mappingType = parameterElt.getStringValue();
                if (!mappingType.equals("exact") && !mappingType.equals("pattern")) {
                    logger.error("mappingType must be exact or pattern but the convertType is " + mappingType);
                    throw new Exception("mappingType must be exact or pattern but the convertType is " + mappingType);
                }
            } else if (parameterName.equals("defaultValue")) {
                defaultValue = parameterElt.getStringValue();
            }
        }
    }

    class Field2Map {

        String fieldName;

        public Field2Map(String pFieldName) {
            fieldName = pFieldName;
        }
    }

    class Field2MapValue {

        String fromValue;
        String toValue;

        public Field2MapValue(String pFromValue, String pToValue) {
            fromValue = pFromValue;
            toValue = pToValue;
        }
    }

    public static void main(String[] args) {
        File inputXml = new File("FieldMapOperator-test-specific.xml");
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
