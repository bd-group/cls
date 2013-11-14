/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.nosqlcluster;

import cn.ac.iie.cls.cc.commons.RuntimeEnv;
import cn.ac.iie.cls.cc.config.Configuration;
import cn.ac.iie.cls.cc.slave.SlaveHandler;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

/**
 *
 * @author wanghh19880807
 */
public class NoSqlClusterTableCreateHandler implements SlaveHandler {

    private static final String SUCCESS_RESPONSE = "<response><message>MESSAGE<message></response>";
    private static final String FAIL_RESPONSE = "<error><message>MESSAGE<message></error>";
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(NoSqlClusterTableCreateHandler.class.getName());
    }

    public String execute(String pRequestContent) {

        if (pRequestContent == null || pRequestContent.isEmpty()) {
            return FAIL_RESPONSE.replace("MESSAGE", "create table unsuccessfully for table creating configuration is empty");
        }

        String databaseName = null;
        String tableName = null;
        String comment = null;
        Connection conn = null;

        try {
            Document databaseDropDoc = DocumentHelper.parseText(pRequestContent);
            Element requestParamsElt = databaseDropDoc.getRootElement();
            Element databaseNameElt = requestParamsElt.element("databaseName");
            databaseName = databaseNameElt == null ? "" : databaseNameElt.getStringValue();
            Element tableNameElt = requestParamsElt.element("tableName");
            tableName = tableNameElt == null ? "" : tableNameElt.getStringValue();
            Element commentElt = requestParamsElt.element("comment");
            comment = commentElt == null ? "" : commentElt.getStringValue();

            Element columnsNameElt = requestParamsElt.element("columns");


            //获取column对象
            List<Element> columnNameElts = columnsNameElt.elements("column");
            //共读取到了多少列
            int columnCount = columnNameElts.size();
            //用来存放name与type的内容
            String[] columnNames = new String[columnCount];
            String[] columnTypes = new String[columnCount];
            int index = 0;
            for (Element columnNameElt : columnNameElts) {
                //获取name对象的内容
                columnNames[index] = columnNameElt.element("name").getStringValue().toLowerCase();
                //获取type对象的内容,并将其进行类型转换
                columnTypes[index] = this.getColumnType(columnNameElt.element("type").getStringValue());
                index++;
            }


            if (databaseName.isEmpty()) {
                return FAIL_RESPONSE.replace("MESSAGE", "databaseName  is not defined");
            } else if (tableName.isEmpty()) {
                return FAIL_RESPONSE.replace("MESSAGE", "tableName is not defined");
            } else if (columnsNameElt == null) {
                return FAIL_RESPONSE.replace("MESSAGE", "columns is not defined");
            } else {
                //connect to hive
                Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
                conn = DriverManager.getConnection((String) RuntimeEnv.getParam(RuntimeEnv.HIVE_CONN_STR), "", "");
                Statement stmt = conn.createStatement();
                //parse xml
                //then create table
                //将获取的数据进行拼接，编写创建表的SQL语句
                String sql = "USE " + databaseName;
                logger.info(sql);
                stmt.executeQuery(sql);

                sql = "CREATE TABLE IF NOT EXISTS " + tableName + "(";
                String bzDDPartitionKey = "";
                for (int i = 0; i < index; i++) {
                    if (columnNames[i].endsWith("dd") && columnTypes[i].equals("INT")) {
                        bzDDPartitionKey = columnNames[i];
                        continue;
                    }
                    if (sql.endsWith("(")) {
                        sql += columnNames[i] + " " + columnTypes[i];
                    } else {
                        sql += "," + columnNames[i] + " " + columnTypes[i];
                    }
                }

                sql += ",cls_input_time timestamp";
                sql += ") COMMENT " + "'" + comment + "'";
                sql += " PARTITIONED BY (" + (bzDDPartitionKey.isEmpty() ? "" : bzDDPartitionKey + " INT,") + "cls_input_time_dd INT)";
                logger.info(sql);
                stmt.executeQuery(sql);

                return SUCCESS_RESPONSE.replace("MESSAGE", "create table " + databaseName + "." + tableName + " successfully");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            return FAIL_RESPONSE.replace("MESSAGE", "create table " + databaseName + "." + tableName + " unsuccessfully for " + ex.getMessage());
        } finally {
            try {
                conn.close();
            } catch (Exception ex) {
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException ex1) {
                        ex1.printStackTrace();
                    }
                }
            }
        }
    }

    public String getColumnType(String oldType) {

        if ("CHAR".equalsIgnoreCase(oldType) || "clob".equalsIgnoreCase(oldType)) {
            return "STRING";
        } else if (oldType.toUpperCase().startsWith("VARCHAR2")) {
            return "STRING";
        } else if ("LONG".equalsIgnoreCase(oldType)) {
            return "BIGINT";
        } else if ("NUMBER".equalsIgnoreCase(oldType)) {
            return "DOUBLE";
        } else if ("INTEGER".equalsIgnoreCase(oldType)) {
            return "INT";
        } else if ("FLOAT".equalsIgnoreCase(oldType)) {
            return "FLOAT";
        } else if ("DATE".equalsIgnoreCase(oldType)) {
            return "TIMESTAMP";
        } else if ("BLOB".equalsIgnoreCase(oldType)) {
            return "BINARY";
        } else {
            return oldType;
        }
    }

    public static void main(String[] args) throws Exception {
        String configurationFileName = "cls-cc.properties";
        logger.info("initializing cls cc server...");
        logger.info("getting configuration from configuration file " + configurationFileName);
        Configuration conf = Configuration.getConfiguration(configurationFileName);
        if (conf == null) {
            throw new Exception("reading " + configurationFileName + " is failed.");
        }

        logger.info("initializng runtime enviroment...");
        try {
            RuntimeEnv.initialize(conf);
        } catch (Exception ex) {
            throw new Exception("initializng runtime enviroment is failed for" + ex.getMessage());
        }
        logger.info("initialize runtime enviroment successfully");
        File inputXml = new File("create-table-specific.xml");
        try {
            String xmlStr = "";
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(inputXml)));
            String line = null;
            while ((line = br.readLine()) != null) {
                xmlStr += line;
            }
            System.out.println(xmlStr);
            System.out.println("OK");
            NoSqlClusterTableCreateHandler handler = new NoSqlClusterTableCreateHandler();
            System.out.println(handler.execute(xmlStr));
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }
}
