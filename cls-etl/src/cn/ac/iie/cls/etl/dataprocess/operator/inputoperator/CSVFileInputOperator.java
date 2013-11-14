/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator.inputoperator;

import cn.ac.iie.cls.etl.cc.slave.etltask.ETLTask;
import cn.ac.iie.cls.etl.dataprocess.commons.RuntimeEnv;
import cn.ac.iie.cls.etl.dataprocess.config.Configuration;
import cn.ac.iie.cls.etl.dataprocess.dataset.DataSet;
import cn.ac.iie.cls.etl.dataprocess.dataset.FieldFactory;
import cn.ac.iie.cls.etl.dataprocess.dataset.Record;
import cn.ac.iie.cls.etl.dataprocess.operator.Operator;
import cn.ac.iie.cls.etl.dataprocess.operator.Port;
import cn.ac.iie.cls.etl.dataprocess.operator.inputoperator.Column.ColumnType;
import cn.ac.iie.cls.etl.dataprocess.util.fs.VFSUtil;
import cn.ac.iie.cls.etl.dataprocess.util.fs.csvfileparser.CSVFileParser;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
public class CSVFileInputOperator extends Operator {

    public static final String OUT_PORT = "outport1";
    public static final String ERROR_PORT = "error1";
    private String csvFile = "";
    private boolean hasHeader = false;
    private boolean trimLines = false;
    private String fileEncoding = "";
    private String enclosure = "";
    private String delimiter = "";
    private String basePath = "";
    private List<Column> columnSet = new ArrayList<Column>();
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(CSVFileInputOperator.class.getName());
    }

    protected void setupPorts() throws Exception {
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
    protected void execute() {
        String[] line = null;
        CSVFileParser cp = null;
        Record record = null;
        String filePathStr = null;
        File inputFile = null;
        try {
            DataSet dataSet = DataSet.getDataSet(columnSet, DataSet.VALID);
//            DataSet errorDataSet = DataSet.getDataSet(columnSet, DataSet.VALID);
            filePathStr = basePath.isEmpty() ? csvFile : basePath + File.separator + csvFile;
            inputFile = VFSUtil.getFile(filePathStr);
            if (inputFile == null) {
                reportExecuteStatus();
                logger.warn("inputFile is null");
                portSet.get(OUT_PORT).write(DataSet.getDataSet(columnSet, DataSet.EOS));
            } else {
                try {
                    cp = new CSVFileParser(inputFile, delimiter, enclosure, fileEncoding, hasHeader, trimLines);
                } catch (Exception ex) {
                    logger.error("Creating file stream is failed for " + ex.getMessage());
                    portSet.get(OUT_PORT).write(DataSet.getDataSet(columnSet, DataSet.EOS));
                }
                while ((line = cp.getNext()) != null) {
//                    if(line[line.length-1]==null&&line.length < columnSet.get(columnSet.size() - 1).columnIdx){
                    if(line.length < columnSet.get(columnSet.size() - 1).columnIdx){    
                      List<String> tmpList=new ArrayList<String>();
                        tmpList.addAll(Arrays.asList(line));
                      for(int i=line.length;i<columnSet.get(columnSet.size() - 1).columnIdx;i++){
                          tmpList.add("null");                         
                      }
                      line=tmpList.toArray(new String[tmpList.size()]);
                    }
                    record = new Record();
//                    if (line.length < columnSet.get(columnSet.size() - 1).columnIdx) {
//                        //fixme
////                        for (int i = 0; i < line.length; i++) {
////                            record.appendField(new StringField(line[i]));
////                        }
////                        errorDataSet.appendRecord(record);
////                        if (errorDataSet.size() >= 1000) {
////                            portSet.get(OUT_PORT).write(errorDataSet);
////                            reportExecuteStatus();
////                            dataSet = DataSet.getDataSet(columnSet, DataSet.VALID);
////                        }
//                        logger.warn("Wrong record in:" + Arrays.asList(line) + "\t the wrong record  in locationfilePath:" + inputFile + "\t the wrong record in  hdfsfilePath:" + filePathStr);
//                        portSet.get(ERROR_PORT).incMetric(1);
//                        continue;
//                    }

                    for (Column column : columnSet) {
                        record.appendField(FieldFactory.getField(line[column.columnIdx - 1], column)); //取出数据
                    }
                    dataSet.appendRecord(record);
                    if (dataSet.size() >= 1000) {
                        portSet.get(OUT_PORT).write(dataSet);
                        reportExecuteStatus();
                        dataSet = DataSet.getDataSet(columnSet, DataSet.VALID);
                    }
                }
//                if (errorDataSet.size() > 0) {
//                    portSet.get(OUT_PORT).write(errorDataSet);
//                    reportExecuteStatus();
//                }
                if (dataSet.size() > 0) {
                    portSet.get(OUT_PORT).write(dataSet);
                    reportExecuteStatus();
                }
            }
            status = SUCCEEDED;
            logger.info("Reading  file is successfully");
        } catch (Exception ex) {
            status = FAILED;
            logger.error("Reading or writing DataSet.EOSfile is failed for " + ex.getMessage(), ex);
        } finally {
            try {
                cp.close();
            } catch (Exception ex) {
                logger.warn("InputFile is null or Creating file stream is failed for " + ex.getMessage());
            }

            try {
                portSet.get(OUT_PORT).write(DataSet.getDataSet(columnSet, DataSet.EOS));
            } catch (Exception ex) {
                logger.warn("Writing DataSet.EOS failed for " + ex.getMessage());
            }
            try {
                inputFile.delete();
            } catch (Exception ex) {
                logger.warn("delete tmpFile " + inputFile + " unsuccessfully for " + ex.getMessage(), ex);
            }

            reportExecuteStatus();
        }
    }

    @Override
    protected void parseParameters(String pParameters) throws Exception {
        logger.info("Start parsing");
        Document document = DocumentHelper.parseText(pParameters);
        Element operatorElt = document.getRootElement();
        Iterator parameterItor = operatorElt.elementIterator("parameter");
        while (parameterItor.hasNext()) {
            Element parameterElement = (Element) parameterItor.next();
            String parameterName = parameterElement.attributeValue("name");
            if (parameterName.endsWith("basePath")) {
                basePath = parameterElement.getStringValue().isEmpty() ? "" : parameterElement.getStringValue().trim();
            } else if (parameterName.equals("csvFile")) {
                csvFile = parameterElement.getStringValue().isEmpty() ? "" : parameterElement.getStringValue().trim();
                if (csvFile.isEmpty()) {
                    logger.error("csvFile is Null!");
                    throw new Exception("csvFile Cannot Be Null!");
                }
            } else if (parameterName.equals("hasHeader")) {
                hasHeader = Boolean.parseBoolean(parameterElement.getStringValue().isEmpty() ? "" : parameterElement.getStringValue().trim());
            } else if (parameterName.equals("trimLines")) {
                trimLines = Boolean.parseBoolean(parameterElement.getStringValue().isEmpty() ? "" : parameterElement.getStringValue().trim());
            } else if (parameterName.equals("fileEncoding")) {
                fileEncoding = parameterElement.getStringValue().isEmpty() ? "" : parameterElement.getStringValue().trim();
                if (fileEncoding.isEmpty()) {
                    logger.error("fileEncoding is Null!");
                    throw new Exception("fileEncoding Cannot Be Null!");
                }
            } else if (parameterName.equals("enclosure")) {
                enclosure = parameterElement.getStringValue().isEmpty() ? " " : parameterElement.getStringValue().trim();
            } else if (parameterName.equals("delimiter")) {
                delimiter = parameterElement.getStringValue().isEmpty() ? "" : parameterElement.getStringValue().trim();
                if (delimiter.isEmpty()) {
                    logger.error("delimiter is Null!");
                    throw new Exception("delimiter Cannot Be Null!");
                }
            } else {
                logger.warn("Wrong parameter configuration");
            }
        }
        Element parameterListElt = operatorElt.element("parameterlist");
        Iterator parametermapItor = parameterListElt.elementIterator("parametermap");
        while (parametermapItor.hasNext()) {
            Element parametermapElt = (Element) parametermapItor.next();
            String columnName = parametermapElt.attributeValue("columnname").isEmpty() ? "" : parametermapElt.attributeValue("columnname").trim();
            int columnIdx = Integer.parseInt(parametermapElt.attributeValue("columnindex").isEmpty() ? "" : parametermapElt.attributeValue("columnindex").trim());
            ColumnType columnType = Column.parseType(parametermapElt.attributeValue("columntype").isEmpty() ? "" : parametermapElt.attributeValue("columntype").trim());
            String format = parametermapElt.attributeValue("format") == null ? "" : parametermapElt.attributeValue("format").trim();
            columnSet.add(new Column(columnName, columnIdx, columnType, format));
        }
        Collections.sort(columnSet);
        logger.info("Parsing  configuration file is successful");
    }

    public static void main(String[] args) throws Exception {
        String configurationFileName = "cls-etl.properties";
        logger.info("initializing cls etl server...");
        logger.info("getting configuration from configuration file " + configurationFileName);
        Configuration conf = Configuration.getConfiguration(configurationFileName);
        if (conf == null) {
            throw new Exception("reading " + configurationFileName + " is failed.");
        }

        logger.info("initializng runtime enviroment...");
        try {
            RuntimeEnv.initialize(conf);
        } catch (Exception ex) {
            throw new Exception("initializng runtime enviroment is failed for " + ex.getMessage());
        }

        logger.info("initialize runtime enviroment successfully");
        File inputXml = new File("CSVFileInputOperator-test-specific.xml");
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
