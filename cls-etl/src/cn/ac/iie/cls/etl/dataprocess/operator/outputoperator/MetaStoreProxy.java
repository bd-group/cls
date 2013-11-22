/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator.outputoperator;

import cn.ac.iie.cls.etl.dataprocess.commons.RuntimeEnv;
import cn.ac.iie.cls.etl.dataprocess.dataset.Record;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author lbh
 */
public class MetaStoreProxy {

    private static Map<String, List<TableColumn>> tableSet = new HashMap<String, List<TableColumn>>();
    private static Map<String, Table> dbTableSet = new HashMap<String, Table>();
    private static BlockingQueue<Connection> connPool = new LinkedBlockingQueue<Connection>();
    private static HiveMetaStoreClient client = null;
    private static Lock clientLock = new ReentrantLock();
    private static int connPoolSize = 0;
    private static Lock connPoolLock = new ReentrantLock();
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(GlobalTableOuputOperator.class.getName());
    }

    public static List getColumnList(String pDatabaseName, String pTableName) throws Exception {
        List<TableColumn> columnSet = null;
        synchronized (tableSet) {
            columnSet = tableSet.get(pDatabaseName + "." + pTableName);
        }
        if (columnSet == null) {
            Connection conn = null;
            Statement stmt = null;
            try {
                conn = getConnection();
                stmt = conn.createStatement();
                columnSet = new ArrayList<TableColumn>();

                String sql = "";
                boolean flag = false;
                for (int i = 0; i < 5; i++) {
                    try {
                        sql = "USE " + pDatabaseName;
                        logger.info(sql);
                        stmt.execute(sql);
                        flag = true;
                        break;
                    } catch (Exception ex) {
                        Thread.sleep(500);
                    }
                }
                if (!flag) {
                    throw new Exception("exe sql " + sql + " unsuccessfully");
                }

                try {
                    sql = "describe " + pTableName;
                    ResultSet rs = stmt.executeQuery(sql);
                    while (rs.next()) {
                        columnSet.add(new TableColumn(rs.getString(1).toLowerCase(), rs.getString(2).toLowerCase()));
                    }
                } catch (Exception ex) {
                    throw new Exception("exe sql " + sql + " unsuccessfully for " + ex.getMessage(), ex);
                }

                synchronized (tableSet) {
                    if (!tableSet.containsKey(pDatabaseName + "." + pTableName)) {
                        tableSet.put(pDatabaseName + "." + pTableName, columnSet);
                    }
                }

                return columnSet;
            } finally {
                try {
                    stmt.close();
                } catch (Exception ex) {
                }
                closeConnection(conn);
            }
        } else {
            return columnSet;
        }
    }

    public static String getOutputFormat(List<TableColumn> pColumnSet, List<Field2TableOutput> pField2TableOutputSet) {

        String outputFormat = "";
        for (TableColumn tableColumn : pColumnSet) {
            if (outputFormat.isEmpty()) {
                outputFormat = "#" + tableColumn.name + "_VAL#";
            } else {
                outputFormat += "\t#" + tableColumn.name + "_VAL#";
            }
        }
        for (Field2TableOutput field2TableOutput : pField2TableOutputSet) {
            outputFormat = outputFormat.replaceAll("#" + field2TableOutput.tableFieldName + "_VAL#", "#" + field2TableOutput.tableFieldName + "_REP#");
        }
        for (TableColumn tableColumn : pColumnSet) {
            if (tableColumn.name.startsWith("cls_")) {
                outputFormat = outputFormat.replaceFirst("#" + tableColumn.name + "_VAL#", "#" + tableColumn.name + "_REP#");
            } else {
                outputFormat = outputFormat.replaceFirst("#" + tableColumn.name + "_VAL#", "\\\\N");
            }
        }
        System.out.println("outputFormat:" + outputFormat);

        return outputFormat;
    }

    public static void truncateTable(String pDatabaseName, String pTableFullName) throws Exception {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = getConnection();
            stmt = conn.createStatement();
            String sql = "USE " + pDatabaseName;
            logger.info(sql);
            stmt.executeQuery(sql);

            String tmpTableName = pTableFullName + "_" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
            sql = "CREATE TABLE " + tmpTableName + " LIKE " + pTableFullName;
            logger.info(sql);
            stmt.executeQuery(sql);

            sql = "DROP TABLE " + pTableFullName;
            logger.info(sql);
            stmt.executeQuery(sql);

            sql = "ALTER TABLE " + tmpTableName + " RENAME TO " + pTableFullName;
            logger.info(sql);
            stmt.executeQuery(sql);
        } catch (Exception ex) {
            logger.error("truncateTable " + pTableFullName + " unsuccessfully for " + ex.getMessage(), ex);
        } finally {
            try {
                stmt.close();
            } catch (Exception ex) {
            }
            closeConnection(conn);
        }
    }

    public static String createTempTable(String pDatabase, String pTableName) throws Exception {
        Connection conn = null;
        Statement stmt = null;
        String tempTableName = "";
        try {
            conn = getConnection();
            stmt = conn.createStatement();

            String sql = "USE " + pDatabase;
            logger.info(sql);
            stmt.executeQuery(sql);
            //     String tempTableName = pTableName + "_" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()) + "_" + UUID.randomUUID().toString().replaceAll("\\-", "_");
            tempTableName = pTableName + "_" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
            List<TableColumn> columnSet = tableSet.get(pDatabase + "." + pTableName);
            String tableFields = "";
            for (TableColumn tableColumn : columnSet) {
                if (tableFields.isEmpty()) {
                    tableFields = tableColumn.name + "\t" + tableColumn.type;
                } else {
                    tableFields += "," + tableColumn.name + "\t" + tableColumn.type;
                }
            }
            sql = "CREATE TABLE " + tempTableName + "(" + tableFields + ") row format delimited fields terminated by '\t'";


            logger.info(sql);
            stmt.executeQuery(sql);

        } catch (Exception ex) {
            logger.error("Create TempTable  " + pTableName + " unsuccessfully for " + ex.getMessage(), ex);
        } finally {
            try {
                stmt.close();
            } catch (Exception ex) {
            }
            closeConnection(conn);
        }
        return tempTableName;
    }

    public static void moveData(String pDatabase, String pTableName, String pTempTableName) throws Exception {
        List<TableColumn> columnSet = tableSet.get(pDatabase + "." + pTableName);
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = getConnection();
            stmt = conn.createStatement();

            String sql = "USE " + pDatabase;
            logger.info(sql);
            stmt.executeQuery(sql);
            String tableFields = "";
            for (TableColumn tableColumn : columnSet) {
                if (tableFields.isEmpty()) {
                    tableFields = tableColumn.name;
                } else {
                    tableFields += "," + tableColumn.name;
                }
            }
            sql = "insert into table " + pTableName + " partition(dd,cls_input_time_dd) select " + tableFields + " from " + pTempTableName;

            logger.info(sql);
            stmt.execute(sql);

        } catch (Exception ex) {
            logger.error("insert into table " + pTableName + " unsuccessfully for " + ex.getMessage(), ex);
        } finally {
            try {
                stmt.close();
            } catch (Exception ex) {
            }
            closeConnection(conn);
        }


    }

    public static void droptable(String pDatabase, String pTableName) throws Exception {

        Connection conn = null;
        Statement stmt = null;
        try {
            conn = getConnection();
            stmt = conn.createStatement();

            String sql = "USE " + pDatabase;
            logger.info(sql);
            stmt.executeQuery(sql);
            sql = "DROP TABLE " + pTableName;
            logger.info(sql);
            stmt.executeQuery(sql);
        } catch (Exception ex) {
            logger.error("drop table " + pTableName + " unsuccessfully for " + ex.getMessage(), ex);
        } finally {
            try {
                stmt.close();
            } catch (Exception ex) {
            }
            closeConnection(conn);
        }
    }

    public static Partition getPartition(String pDbName, String pTableName, List<String> pValues) throws Exception {
        Partition partition = new Partition();
        Table table = null;
        try {
            synchronized (dbTableSet) {
                table = dbTableSet.get(pDbName + "." + pTableName);
                if (table == null) {
                    //fixme           table==null
                    HiveMetaStoreClient msClient = null;
                    try {
                        msClient = MetaStoreProxy.getHiveMetaStoreClient();
                        table = msClient.getTable(pDbName, pTableName);
                        dbTableSet.put(pDbName + "." + pTableName, table);
                    } finally {
                        try {
                            msClient.close();
                        } catch (Exception ex) {
                        }
                    }
                }
            }
            partition.setDbName(pDbName);
            partition.setTableName(pTableName);
            partition.setValues(pValues);

//            StorageDescriptor tableSD = table.getSd();
//            StorageDescriptor partSD = new StorageDescriptor();
//            partSD.setCols(tableSD.getCols());
//            partSD.setCompressed(tableSD.isCompressed());
//            partSD.setOutputFormat(tableSD.getOutputFormat());
//            partSD.setInputFormat(tableSD.getInputFormat());
//            partSD.setSerdeInfo(tableSD.getSerdeInfo());
//            partSD.setLocation(tableSD.getLocation() + "/dd=" + pValues.get(0) + "/cls_input_time_dd=" + pValues.get(1));
//            partition.setSd(partSD);


            Table tmpTalbe = (Table) MetaStoreProxy.copy(table);
            partition.setSd(tmpTalbe.getSd());
//            System.out.println("000000000000\t" + tmpTalbe.getSd().getLocation());
            partition.getSd().setSerdeInfo(tmpTalbe.getSd().getSerdeInfo());
            partition.getSd().setLocation(tmpTalbe.getSd().getLocation() + "/dd=" + pValues.get(0) + "/cls_input_time_dd=" + pValues.get(1));
//            System.out.println("1111111\t" + tmpTalbe.getSd().getLocation());

        } catch (Exception ex) {
            //fixme
            logger.warn("add partition to " + pTableName + "unsuccessfully");
            ex.printStackTrace();
        }
        return partition;
    }

    private static Object copy(Table pTable) throws IOException, ClassNotFoundException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(pTable);
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()));
        return ois.readObject();
    }

    public static void addpartitions(List<Partition> pPartitionSet) throws Exception {
        HiveMetaStoreClient msClient = null;
        try {
            msClient = MetaStoreProxy.getHiveMetaStoreClient();
            for (Partition part : pPartitionSet) {
                try {
                    msClient.add_partition(part);
                } catch (AlreadyExistsException ex) {
                } catch (Exception ex) {
                }
            }
        } catch (Exception ex) {
            logger.warn("add partition unsuccessfully for " + ex.getMessage(), ex);
        } finally {
            try {
                msClient.close();
            } catch (Exception ex) {
            }
        }
    }

    private static Connection getConnection() throws Exception {
        Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
        return DriverManager.getConnection((String) RuntimeEnv.getParam(RuntimeEnv.HIVE_CONN_STR), "", "");
    }

    private static void closeConnection(Connection conn) {
        try {
            conn.close();
        } catch (Exception ex) {
        }
    }

    private static HiveMetaStoreClient getHiveMetaStoreClient() throws Exception {
        HiveMetaStoreClient msClient = null;
        try {
//            clientLock.lock();
//            if (client == null) {
            HiveConf hiveConf = new HiveConf();
            hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, RuntimeEnv.META_STORE_CONN_STR);
//            hiveConf.setVar(HiveConf.ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY, "5000");
//            hiveConf.setVar(HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT, "50000");
//                client = new HiveMetaStoreClient(hiveConf);
//                return client;
            msClient = new HiveMetaStoreClient(hiveConf);
//            }
        } catch (Exception ex) {
        } finally {
//            clientLock.unlock();
        }
//        return client;
        return msClient;
    }

    private static void closeHiveMetaStoreClient(HiveMetaStoreClient pClient) {
        pClient.close();
    }
//    private static Connection getConnection() throws Exception {
//        try {
//            connPoolLock.lock();
//            Connection conn = connPool.peek();
//            if (conn == null) {
//                if (connPoolSize < 6) {
//                    boolean succeed = false;
//                    for (int tt = 0; tt < 5; tt++) {
//                        try {
//                            Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
//                            conn = DriverManager.getConnection((String) RuntimeEnv.getParam(RuntimeEnv.HIVE_CONN_STR), "", "");
//                            succeed = true;
//                            break;
//                        } catch (Exception ex) {
//                        }
//                    }
//
//                    if (succeed) {
//                        connPoolSize++;
//                        return conn;
//                    } else {
//                        logger.warn("init connection to hive unsuccessfully");
//                        return null;
//                    }
//                } else {
//                    logger.info("waiting for available connection...");
//                    return connPool.take();
//                }
//            } else {
//                return connPool.take();
//            }
//        } finally {
//            connPoolLock.unlock();
//        }
//    }
//    private static void closeConnection(Connection conn) {
//        try {
//            connPool.put(conn);
//        } catch (Exception ex) {
//        }
//    }
}
