/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.nosqlcluster;

import cn.ac.iie.cls.cc.commons.RuntimeEnv;
import cn.ac.iie.cls.cc.slave.SlaveHandler;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

/**
 *
 * @author wanghh19880807
 */
public class NoSqlClusterTableInfoHandler implements SlaveHandler {

    private static final String SUCCESS_RESPONSE = "<response><message>MESSAGE<message></response>";
    private static final String FAIL_RESPONSE = "<error><message>MESSAGE<message></error>";
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(NoSqlClusterTableInfoHandler.class.getName());
    }

    public String execute(String pRequestContent) {

        if (pRequestContent == null || pRequestContent.isEmpty()) {
            return FAIL_RESPONSE.replace("MESSAGE", "get table information unsuccessfully for table information getting configuration is empty");
        }

        String databaseName = null;
        String tableName = null;
        Connection conn = null;
        ResultSet rs = null;
        try {
            Document databaseDropDoc = DocumentHelper.parseText(pRequestContent);
            Element requestParamsElt = databaseDropDoc.getRootElement();
            Element databaseNameElt = requestParamsElt.element("databaseName");
            databaseName = databaseNameElt == null ? "" : databaseNameElt.getStringValue();
            Element tableNameElt = requestParamsElt.element("tableName");
            tableName = tableNameElt == null ? "" : tableNameElt.getStringValue();

            if (databaseName.isEmpty()) {
                return FAIL_RESPONSE.replace("MESSAGE", "databaseName  is not defined");
            } else if (tableName.isEmpty()) {
                return FAIL_RESPONSE.replace("MESSAGE", "tableName is not defined");
            } else {
                //connect to hive
                Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
                conn = DriverManager.getConnection((String) RuntimeEnv.getParam(RuntimeEnv.HIVE_CONN_STR), "", "");
                Statement stmt = conn.createStatement();
                //parse xml
                //then create table
                String sql = "USE " + databaseName;
                logger.info(sql);
                stmt.executeQuery(sql);

                sql = "Describe " + tableName;
                logger.info(sql);
                rs = stmt.executeQuery(sql);
                String message = "<databaseName>" + databaseName + "</databaseName>" + "<tableName>" + tableName + "</tableName>" + "	<tableType></tableType>" + "<columns>";
                //拼接结果
                while (rs.next()) {
                    //获取列名称
                    message += "<column>" + "<name>" + rs.getString(1) + "</name>" + "<type>" + rs.getString(2) + "</type>"
                            + "<default />" + "<comment />" + "<null></null>" + "<pk></pk>"
                            + "<index></index>" + "<unique></unique>" + "</column>";
                }
                message += "</columns>";

                return SUCCESS_RESPONSE.replace("MESSAGE", message);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            return FAIL_RESPONSE.replace("MESSAGE", "get information of table " + databaseName + "." + tableName + " unsuccessfully for " + ex.getMessage());
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

    public static void main(String[] args) {
        File inputXml = new File("info-table-specific.xml");
        try {
            String xmlStr = "";
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(inputXml)));
            String line = null;
            while ((line = br.readLine()) != null) {
//                System.out.println(line);
                xmlStr += line;
            }
            System.out.println(xmlStr);
            System.out.println("OK");
            NoSqlClusterTableInfoHandler handler = new NoSqlClusterTableInfoHandler();
            System.out.println(handler.execute(xmlStr));
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }
}
