/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator.outputoperator;

import cn.ac.iie.cls.etl.cc.slave.etltask.ETLTask;
import cn.ac.iie.cls.etl.dataprocess.commons.RuntimeEnv;
import cn.ac.iie.cls.etl.dataprocess.config.Configuration;
import cn.ac.iie.cls.etl.dataprocess.dataset.DataSet;
import cn.ac.iie.cls.etl.dataprocess.dataset.Field;
import cn.ac.iie.cls.etl.dataprocess.dataset.Record;
import cn.ac.iie.cls.etl.dataprocess.operator.Operator;
import cn.ac.iie.cls.etl.dataprocess.operator.Port;
import cn.ac.iie.cls.etl.dataprocess.util.fs.VFSUtil;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

/**
 *
 * @author root
 */
public class DictionaryTableSynchronizationOperator extends Operator {

    public static final String IN_PORT = "inport1";
    public static final String OUT_PORT = "outport1";
    public static final String ERROR_PORT = "errport1";
    private String tableName;
    private String dicTableName;
    private String datasourceListStr;
    private List<Field2TableOutput> field2TableOutputSet = new ArrayList<Field2TableOutput>();
    private List<String> datasourceList = new ArrayList<String>();
    private String outputFormat = "";
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(GlobalTableOuputOperator.class.getName());
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

        SimpleDateFormat dayFormatSDF = new SimpleDateFormat("yyyyMMddHHmmss");
        SimpleDateFormat timestampSDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Map<String, String> partitionSet = new HashMap<String, String>();
        List<Partition> partitions = new ArrayList<Partition>();
        Map<String, String> ds2fileSet = new HashMap<String, String>();
        Map<String, BufferedWriter> ds2bwSet = new HashMap<String, BufferedWriter>();
        boolean isPartition = false;
        try {
            
            for (String dataSource : datasourceList) {
                MetaStoreProxy.createTempTable(dataSource, tableName,dicTableName);
            }
            if (MetaStoreProxy.getPartitionKeySize(datasourceList.get(0), dicTableName) != 0) {
                isPartition = true;
            }
            File file = new File(RuntimeEnv.getParam(RuntimeEnv.TMP_DATA_DIR).toString());
            if (!file.exists()) {
                file.mkdirs();
            }
            while (true) {
                DataSet dataSet = portSet.get(IN_PORT).getNext();
                for (int i = 0; i < dataSet.size(); i++) {
                    Record record = dataSet.getRecord(i);
                    String outString = outputFormat;
                    try {
                        for (Field2TableOutput field2TableOutput : field2TableOutputSet) {
                            if (!field2TableOutput.streamFieldName.startsWith("cls_")) {
                                Field field = record.getField(field2TableOutput.streamFieldName);
                                String fieldVal = field == null ? null : field.toString();
                                if (fieldVal == null) {
                                    outString = outString.replaceFirst("#" + field2TableOutput.tableFieldName + "_REP#", "\\\\N");
                                } else {
                                    //dispose \t
                                    fieldVal = fieldVal.replaceAll("\t", " ");
                                    //dispose \
                                    fieldVal = fieldVal.replaceAll("\\\\", "\\\\\\\\");
                                    //dispose $
                                    fieldVal = fieldVal.replaceAll("\\$", "DOLLAR_IREP");
                                    outString = outString.replaceFirst("#" + field2TableOutput.tableFieldName + "_REP#", fieldVal);
                                    outString = outString.replaceAll("DOLLAR_IREP", "\\$");
                                }
                            }
                        }
                    } catch (Exception ex) {
                        logger.warn("parse record " + record + " unsuccessfully for " + ex.getMessage(), ex);
                        continue;
                    }
                    Date nowTime = new Date();
                    outString = outString.replaceFirst("#cls_input_time_REP#", timestampSDF.format(nowTime));
                    outString = outString.replaceFirst("#cls_input_time_dd_REP#", dayFormatSDF.format(nowTime).substring(0, 8));


                    for (String datasource : datasourceList) {
                        if (datasource != null) {
                            if (isPartition) {
                                String hdfsFilepath = datasource + ".db/" + tableName + "/dd=" + record.getField("DD") + "/cls_input_time_dd=" + dayFormatSDF.format(nowTime).substring(0, 8);

                                BufferedWriter bw = ds2bwSet.get(hdfsFilepath);
                                if (bw == null) {
                                    String tmpDataFilePath = RuntimeEnv.getParam(RuntimeEnv.TMP_DATA_DIR) + File.separator + tableName + "_" + dayFormatSDF.format(new Date()) + "_" + UUID.randomUUID() + "_" + datasource;
                                    bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(tmpDataFilePath)), "utf-8"));
                                    ds2bwSet.put(hdfsFilepath, bw);
                                    ds2fileSet.put(hdfsFilepath, tmpDataFilePath);
                                }
                                bw.write(outString + "\n");

                                List<String> value = new ArrayList<String>();
                                value.add(record.getField("DD").toString());
                                value.add(dayFormatSDF.format(nowTime).substring(0, 8));
                                String partitionKey = record.getField("DD").toString() + dayFormatSDF.format(nowTime).substring(0, 8);
                                if (!partitionSet.containsKey(partitionKey)) {
                                    partitions.add(MetaStoreProxy.getPartition(datasource, dicTableName, value));
                                    partitionSet.put(partitionKey, partitionKey);
                                }
                            } else {
                                String hdfsPath = datasource + ".db/" + tableName;

                                BufferedWriter bw = ds2bwSet.get(hdfsPath);
                                if (bw == null) {
                                    String tmpDataFilePath = RuntimeEnv.getParam(RuntimeEnv.TMP_DATA_DIR) + File.separator + tableName + "_" + dayFormatSDF.format(new Date()) + "_" + UUID.randomUUID() + "_" + datasource;
                                    bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(tmpDataFilePath)), "utf-8"));
                                    ds2bwSet.put(hdfsPath, bw);
                                    ds2fileSet.put(hdfsPath, tmpDataFilePath);
                                }
                                bw.write(outString + "\n");
                            }
                        }
                    }
                }
                if (dataSet.isValid()) {
                    portSet.get(OUT_PORT).incMetric(dataSet.size());
                    reportExecuteStatus();
                } else {
                    for (String hdfsPath : ds2fileSet.keySet()) {
                        BufferedWriter bw = ds2bwSet.get(hdfsPath);
                        bw.close();
                        try {
                            VFSUtil.putFile(ds2fileSet.get(hdfsPath), RuntimeEnv.getParam(RuntimeEnv.HDFS_CONN_STR) + "/user/hive/warehouse/" + hdfsPath.replaceFirst(tableName, dicTableName) + "/");
                            File tempFile = new File(ds2fileSet.get(hdfsPath));
                            tempFile.delete();
                        } catch (Exception ex) {
                            logger.warn("upload file to hadfs unsuccessfully,the file is " + ds2fileSet.get(hdfsPath));
                            throw new Exception("upload file to hadfs unsuccessfully " + ex.getMessage(), ex);
                        }
                    }
                    if (isPartition) {
                        MetaStoreProxy.addpartitions(partitions);
                    }
                    break;
                }

            }
            status = SUCCEEDED;
        } catch (Exception ex) {
            status = FAILED;
            logger.error("some error happened when doing table output for " + ex.getMessage(), ex);
        } finally {
            reportExecuteStatus();
        }
    }

    @Override
    protected void parseParameters(String pParameters) throws Exception {
        Document document = DocumentHelper.parseText(pParameters);
        Element operatorElt = document.getRootElement();
        Iterator parameterItor = operatorElt.elementIterator("parameter");
        while (parameterItor.hasNext()) {
            Element parameterElement = (Element) parameterItor.next();
            String parameterName = parameterElement.attributeValue("name");
            if (parameterName.equals("datasourceList")) {
                datasourceListStr = parameterElement.getStringValue();
                if (datasourceListStr.isEmpty()) {
                    logger.warn("datasourceList can not be null");
                    throw new Exception("datasourceList is null");
                }
            } else if (parameterName.equals("tableName")) {
                tableName = parameterElement.getStringValue();
                if (tableName.isEmpty()) {
                    logger.warn("tableName can not be null");
                    throw new Exception("tableName is null");
                }
            }else if (parameterName.equals("dictableName")) {
                dicTableName = parameterElement.getStringValue();
                if (tableName.isEmpty()) {
                    logger.warn("dicTableName can not be null");
                    throw new Exception("dicTableName is null");
                }
            }
        }
        parameterItor = operatorElt.element("parameterlist").elementIterator("parametermap");
        while (parameterItor.hasNext()) {
            Element paraMapElt = (Element) parameterItor.next();
            field2TableOutputSet.add(new Field2TableOutput(paraMapElt.attributeValue("streamfield").trim(), paraMapElt.attributeValue("tablefield").toLowerCase().trim()));
        }

        field2TableOutputSet.add(new Field2TableOutput("cls_input_time", "cls_input_time"));
        field2TableOutputSet.add(new Field2TableOutput("cls_input_time_dd", "cls_input_time_dd"));

//        String datasourceListStr = RuntimeEnv.getParam(RuntimeEnv.DATASOURCE_LIST).toString();
        String[] datasourceStrs = datasourceListStr.split(",");
        datasourceList.addAll(Arrays.asList(datasourceStrs));

        List<TableColumn> columnSet = null;
        for (String datasourceName : datasourceList) {
            columnSet = MetaStoreProxy.getColumnList(datasourceName, tableName);
        }

        outputFormat = MetaStoreProxy.getOutputFormat(columnSet, field2TableOutputSet);

        logger.info("Argument parsing is successful");
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
        File inputXml = new File("tableOutputOperator-test-specific.xml");
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
