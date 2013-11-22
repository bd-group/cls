/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator.outputoperator;

import cn.ac.iie.cls.etl.cc.slave.etltask.ETLTask;
import cn.ac.iie.cls.etl.dataprocess.commons.RuntimeEnv;
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
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
public class GlobalTableOuputOperator extends Operator {

    public static final String IN_PORT = "inport1";
    public static final String OUT_PORT = "outport1";
    public static final String ERROR_PORT = "errport1";
    private List<String> datasourceList = new ArrayList<String>();
    private String tableName;
    private boolean syncOutput;
    private String compareFiledLogicExp;
    private List<DatasourceDivider> datasourceDividerSet = new ArrayList<DatasourceDivider>();
    private List<Field2TableOutput> field2TableOutputSet = new ArrayList<Field2TableOutput>();
    private String outputFormat = "";
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(GlobalTableOuputOperator.class.getName());
    }

    @Override
    protected void setupPorts() throws Exception {
        setupPort(new Port(Port.INPUT, IN_PORT));
        setupPort(new Port(Port.OUTPUT, OUT_PORT));
        setupPort(new Port(Port.OUTPUT, ERROR_PORT));
    }

    @Override
    protected void parseParameters(String pParameters) throws Exception {

        Document document = DocumentHelper.parseText(pParameters);
        Element operatorElt = document.getRootElement();
        Iterator parameterItor = operatorElt.elementIterator("parameter");
        while (parameterItor.hasNext()) {
            Element parameterElement = (Element) parameterItor.next();
            String parameterName = parameterElement.attributeValue("name");
            if (parameterName.equals("tableName")) {
                tableName = parameterElement.getStringValue().trim();
                if (tableName.isEmpty()) {
                    logger.error("tableName is null");
                    throw new Exception("tableName connot be null!");
                }
            } else if (parameterName.equals("syncOutput")) {
                syncOutput = Boolean.parseBoolean(parameterElement.getStringValue());
            } else if (parameterName.equals("compareFieldLogicExp")) {
                compareFiledLogicExp = parameterElement.getStringValue().trim();
                if (compareFiledLogicExp.isEmpty()) {
                    logger.error("compareFiledLogicExp is null");
                    throw new Exception("compareFiledLogicExp connot be null");
                }
            } else {
                logger.warn("Wrong parameter configuration");
            }
        }

        parameterItor = operatorElt.elementIterator("parameterlist");

        while (parameterItor.hasNext()) {
            Element parameterlistElt = (Element) parameterItor.next();
            if (parameterlistElt.attributeValue("name").equals("mapRules")) {
                Iterator mapRulesItor = parameterlistElt.elementIterator("parametermap");
                while (mapRulesItor.hasNext()) {
                    Element mapRuleElt = (Element) mapRulesItor.next();
                    String mapKey = mapRuleElt.attributeValue("mapKey").trim();
                    String datasource = mapRuleElt.attributeValue("datasource").trim();
                    if (mapKey.isEmpty() || datasource.isEmpty()) {
                        logger.error("mapKey or datasource is null");
                        throw new Exception("mapKey and datasource connot be null");
                    }
                    datasourceList.add(datasource);
                    setupPort(new Port(Port.OUTPUT, datasource));
                    DatasourceDivider datasourceDivider = new DatasourceDivider(mapKey, datasource);
                    datasourceDividerSet.add(datasourceDivider);
                }
            } else if (parameterlistElt.attributeValue("name").equals("fields")) {
                Iterator fieldsItor = parameterlistElt.elementIterator("parametermap");
                while (fieldsItor.hasNext()) {
                    Element fieldElt = (Element) fieldsItor.next();
                    String srteamField = fieldElt.attributeValue("streamfield").trim();
                    String tableField = fieldElt.attributeValue("tablefield").toLowerCase().trim();
                    field2TableOutputSet.add(new Field2TableOutput(srteamField, tableField));
                }
            }
        }
        field2TableOutputSet.add(new Field2TableOutput("cls_input_time", "cls_input_time"));
        field2TableOutputSet.add(new Field2TableOutput("cls_input_time_dd", "cls_input_time_dd"));

        //validate
        List<TableColumn> columnSet = null;
        for (String datasourceName : datasourceList) {
            columnSet = MetaStoreProxy.getColumnList(datasourceName, tableName);
        }

        outputFormat = MetaStoreProxy.getOutputFormat(columnSet, field2TableOutputSet);

        logger.info("Argument parsing is successful");
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
        Map<String, BufferedWriter> ds2bwSet = new HashMap<String, BufferedWriter>();
        Map<String, String> ds2fileSet = new HashMap<String, String>();


        List<Partition> addPartitions = new ArrayList<Partition>();


        Map<String, String> partitionSet = new HashMap<String, String>();

        SimpleDateFormat dayFormatSDF = new SimpleDateFormat("yyyyMMddHHmmss");
        SimpleDateFormat timestampSDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            while (true) {
                DataSet dataSet = portSet.get(IN_PORT).getNext();
                List<String> fieldNameList = dataSet.getFieldNameList();

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
                                    fieldVal = fieldVal.replaceAll("\\\\", "\\\\\\\\\\\\\\\\");
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
                    String compareFieldContent = compareFiledLogicExp;

                    for (String fieldName : fieldNameList) {
                        Field field = record.getField(fieldName);
                        String fieldVal = field == null ? null : field.toString();
                        compareFieldContent = compareFieldContent.replaceAll(fieldName, fieldVal == null ? "" : fieldVal);
                    }
                    int size = datasourceDividerSet.size();
                    String datasource = null;
                    boolean flag = false;
                    String tmpDataFilePath = "";
                    for (int k = 0; k < size; k++) {
                        DatasourceDivider datasourceDivider = datasourceDividerSet.get(k);
                        datasource = datasourceDivider.getDataSource(compareFieldContent);
                        if (datasource != null) {
                            String bwKey = datasource + ".db/" + tableName + "/dd=" + record.getField("DD") + "/cls_input_time_dd=" + dayFormatSDF.format(nowTime).substring(0, 8);
                            List<String> value = new ArrayList<String>();
                            value.add(record.getField("DD").toString());
                            value.add(dayFormatSDF.format(nowTime).substring(0, 8));
                            String partitionKey = record.getField("DD").toString() + dayFormatSDF.format(nowTime).substring(0, 8);
                            if (!partitionSet.containsKey(partitionKey)) {
                                addPartitions.add(MetaStoreProxy.getPartition(datasource, tableName, value));
                                partitionSet.put(partitionKey, partitionKey);
                            }

                            BufferedWriter bw = ds2bwSet.get(bwKey);
                            if (bw == null) {
                                File file = new File(RuntimeEnv.getParam(RuntimeEnv.TMP_DATA_DIR).toString());
                                if (!file.exists()) {
                                    file.mkdirs();
                                }
                                tmpDataFilePath = RuntimeEnv.getParam(RuntimeEnv.TMP_DATA_DIR) + File.separator + tableName + "_" + dayFormatSDF.format(new Date()) + "_" + UUID.randomUUID() + "_" + datasource;

                                bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(tmpDataFilePath)), "utf-8"));
                                ds2bwSet.put(bwKey, bw);
                                ds2fileSet.put(bwKey, tmpDataFilePath);
                            }
                            bw.write(outString + "\n");
                            portSet.get(datasource).incMetric(1);
                            flag = true;
                        }
                    }

                    if (!flag) {
                        //fixme
                    }
                }

                if (dataSet.isValid()) {
                    portSet.get(OUT_PORT).incMetric(dataSet.size());
                    reportExecuteStatus();
                } else {
                	
                    for (String bwkey : ds2bwSet.keySet()) {
                        try {
                            BufferedWriter bw = ds2bwSet.get(bwkey);

                            bw.close();
                            
                            //create temp table
//                            String tempTableName = MetaStoreProxy.createTempTable(datasource, tableName);
                            VFSUtil.putFile(ds2fileSet.get(bwkey), RuntimeEnv.getParam(RuntimeEnv.HDFS_CONN_STR) + "/user/hive/warehouse/" + bwkey + "/");
                            //move data from temp table to real table
//                            MetaStoreProxy.moveData(datasource, tableName, tempTableName);
                            //drop temp table
//                            MetaStoreProxy.droptable(datasource, tempTableName);

                            //delete file related to bw
                            File tempFile = new File(ds2fileSet.get(bwkey));
                            tempFile.delete();
                        } catch (Exception ex) {
                            //fixme
                            logger.warn("upload file to hadfs unsuccessfully " + ex.getMessage(), ex);
                        }
                    }                    
                    MetaStoreProxy.addpartitions(addPartitions);
                    break;
                }
            }
            status = SUCCEEDED;
        } catch (Exception ex) {
            status = FAILED;
            logger.error("some error happened when doing global table output for " + ex.getMessage(), ex);
        } finally {
            for (String bwkey : ds2bwSet.keySet()) {
                try {
                    BufferedWriter bw = ds2bwSet.get(bwkey);
                    bw.close();
                } catch (Exception ex) {
                }
            }
            reportExecuteStatus();
        }
    }

    class DatasourceDivider {

        Pattern dividePattern;
        String datasource;

        public DatasourceDivider(String pDivideRegxStr, String pDataSource) {
            dividePattern = Pattern.compile(pDivideRegxStr);
            datasource = pDataSource;
        }

        public String getDataSource(String pContent) {
            Matcher matcher = dividePattern.matcher(pContent);
            if (matcher.find()) {
                return datasource;
            } else {
                return null;
            }
        }
    }

    public static void main(String[] args) {
        File inputXml = new File("GlobalTableOuputOperator-test-specific.xml");
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
