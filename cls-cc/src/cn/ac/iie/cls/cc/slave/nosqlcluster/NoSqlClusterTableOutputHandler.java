/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.nosqlcluster;

import cn.ac.iie.cls.cc.slave.SlaveHandler;
import cn.ac.iie.cls.cc.slave.clsagent.DataCollectJob;
import cn.ac.iie.cls.cc.slave.clsagent.DataCollectJobTracker;
import cn.ac.iie.cls.cc.slave.dataetl.ETLJob;
import cn.ac.iie.cls.cc.slave.dataetl.ETLJobTracker;
import cn.ac.iie.cls.cc.slave.dataetl.ETLTask;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

/**
 *
 * @author root
 */
public class NoSqlClusterTableOutputHandler implements SlaveHandler {

    private static final String REQUEST_CONTENT =
            "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n"
            + "<requestParams>\n"
            + "    <processJobInstanceId>PROCESS_JOB_INSEANCE_ID</processJobInstanceId>\n"
            + "    <processConfig>\n"
            + "        <operator name=\"table_output_field1\" class=\"Process\" version=\"1.0\" x=\"-1\" y=\"-1\">                              \n"
            + "            <operator name=\"csv_file_reader1\" class=\"CSVFileInput\" version=\"1.0\" x=\"-1\" y=\"-1\">   \n"
            + "                <parameter name=\"basePath\"></parameter>\n"
            + "                <parameter name=\"csvFile\">FILE_FULL_PATH</parameter>                \n"
            + "                <parameter name=\"hasHeader\">false</parameter>                \n"
            + "                <parameter name=\"trimLines\">true</parameter>                \n"
            + "                <parameter name=\"fileEncoding\">UTF-8</parameter>  \n"
            + "                <parameter name=\"delimiter\">,</parameter>              \n"
            + "                <parameter name=\"enclosure\">\"</parameter>\n"
            + "                <parameterlist name=\"columnSet\">                    \n"
            + "                   FILE_INPUT_PARAMETERMAP            \n"
            + "                </parameterlist>                                         \n"
            + "            </operator>             \n"
            + "            <operator name=\"table_output1\" alias=\"数据库表输出\" class=\"TableOutput\">              \n"
            + "                <parameter name=\"datasource\">DATA_BASE_NAME</parameter>\n"
            + "                <parameter name=\"tableName\">TABLE_NAME</parameter>\n"
            + "                <parameter name=\"isClean\">IS_CLEAN</parameter>\n"
            + "                <parameterlist name=\"fields\">\n"
            + "                   FILE_OUTPUT_PARAMETERMAP \n"
            + "                </parameterlist>\n"
            + "            </operator> \n"
            + "            <connect from=\"csv_file_reader1.outport1\" to=\"table_output1.inport1\"/>\n"
            + "        </operator>     \n"
            + "    </processConfig>\n"
            + "</requestParams>\n";
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(NoSqlClusterTableOutputHandler.class.getName());
    }

    @Override
    public String execute(String pRequestContent) {

        String filePath = null;
        String databaseName = null;
        String tableName = null;
        String isClean = null;

        try {
            Document databaseDropDoc = DocumentHelper.parseText(pRequestContent);

            Element requestParamsElt = databaseDropDoc.getRootElement();
            Element databaseNameElt = requestParamsElt.element("databaseName");
            databaseName = databaseNameElt == null ? "" : databaseNameElt.getStringValue();
            Element tableNameElt = requestParamsElt.element("tableName");
            tableName = tableNameElt == null ? "" : tableNameElt.getStringValue();

            Element filePathElt = requestParamsElt.element("filePath");
            filePath = filePathElt == null ? "" : filePathElt.getStringValue();
            Element isCleanElt = requestParamsElt.element("isClean");
            isClean = isCleanElt == null ? "false" : isCleanElt.getStringValue();

            Element columnsNameElt = requestParamsElt.element("relations");
            List<Element> columnNameElts = columnsNameElt.elements("relation");
            int columnCount = columnNameElts.size();
            String[] columnIndex = new String[columnCount];
            String[] columnNames = new String[columnCount];
            String[] columnTypes = new String[columnCount];
            String[] columnFormat = new String[columnCount];
            int index = 0;
            for (Element columnNameElt : columnNameElts) {
                columnNames[index] = columnNameElt.element("tableColumnName").getStringValue().toLowerCase();
                columnTypes[index] = columnNameElt.element("fileColumnType").getStringValue();
                columnIndex[index] = columnNameElt.element("fileColumnIndex").getStringValue();
                columnFormat[index] = columnNameElt.element("format") == null ? "" : columnNameElt.element("format").getStringValue();
                index++;
            }

            String fileInputParametermap = "";
            for (int i = 0; i < columnIndex.length; i++) {
                fileInputParametermap = fileInputParametermap + "<parametermap columntype=\"" + columnTypes[i] + "\" columnname=\"" + columnNames[i] + "\" columnindex=\"" + columnIndex[i] + "\" format=\"" + columnFormat[i] + "\"/>\n";
            }

            String fileOutputParametermap = "";
            for (int i = 0; i < columnNames.length; i++) {
                fileOutputParametermap = fileOutputParametermap + "<parametermap tablefield=\"" + columnNames[i] + "\" streamfield=\"" + columnNames[i] + "\"/>\n";
            }
            pRequestContent = REQUEST_CONTENT;
            pRequestContent = pRequestContent.replaceAll("FILE_FULL_PATH", filePath);
            pRequestContent = pRequestContent.replaceAll("DATA_BASE_NAME", databaseName);
            pRequestContent = pRequestContent.replaceAll("TABLE_NAME", tableName);
            pRequestContent = pRequestContent.replaceAll("IS_CLEAN", isClean);
            pRequestContent = pRequestContent.replaceAll("PROCESS_JOB_INSEANCE_ID", UUID.randomUUID().toString());
            pRequestContent = pRequestContent.replaceAll("FILE_INPUT_PARAMETERMAP", fileInputParametermap);
            pRequestContent = pRequestContent.replaceAll("FILE_OUTPUT_PARAMETERMAP", fileOutputParametermap);

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        String result = "";
        ETLJob etlJob = ETLJob.getETLJob(pRequestContent,false);
        if (etlJob != null) {
            String clsAgentDataCollectDescriptor = etlJob.getDataProcessDescriptor().get(ETLJob.CLS_AGENT_DATA_COLLECT_DESC);
            System.out.println(clsAgentDataCollectDescriptor);
            if (clsAgentDataCollectDescriptor != null) {
                DataCollectJob dataCollectJob = DataCollectJob.getDataCollectJob(clsAgentDataCollectDescriptor);
                dataCollectJob.setEtlJob(etlJob);
                DataCollectJobTracker.getDataCollectJobTracker().appendJob(dataCollectJob);
            }

            ETLJobTracker.getETLJobTracker().appendJob(etlJob);
            if (clsAgentDataCollectDescriptor == null) {
                String inputFilePath = etlJob.getInputFilePathStr();
                System.out.println("#####inputFilePath:" + inputFilePath);
                List<ETLTask> etlTaskList = new ArrayList<ETLTask>();
                etlTaskList.add(new ETLTask(inputFilePath, ETLTask.EXECUTING));
                etlJob.setTask2doNum(1);
                etlJob.appendTask(etlTaskList);
            }


            while (true) {
                try {

                    switch (etlJob.getJobStatus()) {
                        case RUNNING:
                            logger.info("table output job is running,wait...");
                            Thread.sleep(1000);
                            break;
                        case SUCCEED:
                            logger.info("table output job complelte successfully");
                            result = "SUCCEED";
                            break;
                        default:
                            logger.error("table output job complelte unsuccessfully");
                            result = "FAILURE";
                            break;
                    }
                    
                    if("SUCCEED".equals(result)){
                        break;
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        } else {
            result = "FAILURE";
        }

        return result;
    }

    public static void main(String[] args) {
        String str = "";
    }
}
