package cn.ac.iie.cls.etl.dataprocess.operator.outputoperator;

import cn.ac.iie.cls.etl.dataprocess.commons.RuntimeEnv;
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
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class MetaStoreProxy {

    private static Map<String, List<TableColumn>> tableSet = new HashMap();
    private static Map<String, Table> dbTableSet = new HashMap();
    private static BlockingQueue<Connection> connPool = new LinkedBlockingQueue();
    private static HiveMetaStoreClient client = null;
    private static Lock clientLock = new ReentrantLock();
    private static int connPoolSize = 0;
    private static Lock connPoolLock = new ReentrantLock();
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(GlobalTableOuputOperator.class.getName());
    }

    public static List getColumnList(String pDatabaseName, String pTableName)
            throws Exception {
        List columnSet = null;
        synchronized (tableSet) {
            columnSet = (List) tableSet.get(pDatabaseName + "." + pTableName);
        }
        if (columnSet == null) {
            Connection conn = null;
            Statement stmt = null;
            try {
                conn = getConnection();
                stmt = conn.createStatement();
                columnSet = new ArrayList();

                String sql = "";
                boolean flag = false;
                for (int i = 0; i < 5; ++i) {
                    try {
                        sql = "USE " + pDatabaseName;
                        logger.info(sql);
                        stmt.execute(sql);
                        flag = true;
                    } catch (Exception ex) {
                        logger.warn("exe sql " + sql + " unsuccessfully for "+ex.getMessage(),ex);
                        Thread.sleep(500L);                  
                    }
                }
                if (!(flag)) {
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
                    if (!(tableSet.containsKey(pDatabaseName + "." + pTableName))) {
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
        }
        return columnSet;
    }

    public static String getOutputFormat(List<TableColumn> pColumnSet, List<Field2TableOutput> pField2TableOutputSet) {
        String outputFormat = "";
        for (TableColumn tableColumn : pColumnSet) {
            if (outputFormat.isEmpty()) {
                outputFormat = "#" + tableColumn.name + "_VAL#";
            } else {
                outputFormat = outputFormat + "\t#" + tableColumn.name + "_VAL#";
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
            throw new Exception("truncateTable " + pTableFullName + " unsuccessfully for " + ex.getMessage(), ex);
        } finally {
            try {
                stmt.close();
            } catch (Exception ex) {
            }
            closeConnection(conn);
        }
    }

    public static void createTempTable(String pDatabase, String pTableName,String pDicTableName) throws Exception {
        Connection conn = null;
        Statement stmt = null;       
        try {
            conn = getConnection();
            stmt = conn.createStatement();
            String sql = "USE " + pDatabase;
            logger.info(sql);
            stmt.executeQuery(sql);

            sql = "CREATE TABLE IF NOT EXISTS " + pDicTableName + " LIKE " + pTableName;
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
                    tableFields = tableFields + "," + tableColumn.name;
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

    public static void droptable(String pDatabase, String pTableName)
            throws Exception {
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
                table = (Table) dbTableSet.get(pDbName + "." + pTableName);
                if (table == null) {
                    HiveMetaStoreClient msClient = null;
                    try {
                        msClient = getHiveMetaStoreClient();
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
            Table tmpTable = (Table) copy(table);
            partition.setSd(tmpTable.getSd());

            partition.getSd().setSerdeInfo(tmpTable.getSd().getSerdeInfo());
            partition.getSd().setLocation(tmpTable.getSd().getLocation() + "/dd=" + ((String) pValues.get(0)) + "/cls_input_time_dd=" + ((String) pValues.get(1)));
        } catch (Exception ex) {
            logger.warn("add partition to " + pTableName + "unsuccessfully");
            ex.printStackTrace();
        }
        return partition;
    }

    public static int getPartitionKeySize(String pDbName, String pTableName) throws Exception {
        HiveMetaStoreClient msClient = null;
        Table table = null;
        try {
            msClient = getHiveMetaStoreClient();
        } catch (Exception ex) {
            logger.warn("Hive Connection unsuccessfully");
            throw new Exception("Hive Connection unsuccessfully for " + ex.getMessage(), ex);
        }
        try {
            table = msClient.getTable(pDbName, pTableName);
        } catch (Exception ex) {
            logger.warn("no table named " + pTableName + " in " + pDbName);
            throw new Exception("no table named " + pTableName + " in " + pDbName + " for " + ex.getMessage(), ex);

        }

        return table.getPartitionKeysSize();
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
            msClient = getHiveMetaStoreClient();
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
        return DriverManager.getConnection((String) RuntimeEnv.getParam("hiveConnStr"), "", "");
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
            HiveConf hiveConf = new HiveConf();
            hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, (String) RuntimeEnv.getParam("metaStoreConnStr"));

            msClient = new HiveMetaStoreClient(hiveConf);
        } catch (Exception ex) {
        } finally {
        }
        return msClient;
    }

    private static void closeHiveMetaStoreClient(HiveMetaStoreClient pClient) {
        pClient.close();
    }
}
