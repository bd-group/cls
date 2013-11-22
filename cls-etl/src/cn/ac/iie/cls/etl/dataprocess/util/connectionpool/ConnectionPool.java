/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.util.connectionpool;

import cn.ac.iie.cls.etl.dataprocess.commons.RuntimeEnv;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 *
 * @author root
 */
public class ConnectionPool {

    @SuppressWarnings("unused")
    private int initConn=5;  //初始化时的连接数
    private int maxConn=10;//连接池的最大连接数
    private List<ConnectionItem> pool = new ArrayList<ConnectionItem>();

    public ConnectionPool(int initConn, int maxConn) throws Exception {
        this.initConn = initConn;
        this.maxConn = maxConn;
        for (int i = 0; i < initConn; i++) {
            Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
            Connection conn=DriverManager.getConnection((String) RuntimeEnv.getParam(RuntimeEnv.HIVE_CONN_STR), "", "");
            pool.add(new ConnectionItem(conn));
        }

    }

    public synchronized Connection getConnection() throws Exception {
        for (ConnectionItem item : pool) {
            if (item.flag) {
                item.flag = false;
                return item;         //返回的不是连接，而是Connection的Wrapper
            }
        }
        if (pool.size() < maxConn) {
            Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
            Connection conn=DriverManager.getConnection((String) RuntimeEnv.getParam(RuntimeEnv.HIVE_CONN_STR), "", "");
            ConnectionItem item = new ConnectionItem(conn);
            pool.add(item);
            return item;
        } else {
            //wait....
            return null;
        }

    }

    class ConnectionItem implements Connection {    //........................

        Connection con;
        boolean flag;

        ConnectionItem(Connection con) {
            this.con = con;
            flag = true;
        }

        public void clearWarnings() throws SQLException {
            // TODO Auto-generated method stub
        }

        public void close() throws SQLException {         ////////////////////////////////
            flag = true;
        }

        public void commit() throws SQLException {
            // TODO Auto-generated method stub
        }

        public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
            // TODO Auto-generated method stub
            return null;
        }

        public Blob createBlob() throws SQLException {
            // TODO Auto-generated method stub
            return null;
        }

        public Clob createClob() throws SQLException {
            // TODO Auto-generated method stub
            return null;
        }

        public NClob createNClob() throws SQLException {
            // TODO Auto-generated method stub
            return null;
        }

        public SQLXML createSQLXML() throws SQLException {
            // TODO Auto-generated method stub
            return null;
        }

        public Statement createStatement() throws SQLException {
            // TODO Auto-generated method stub
            return null;
        }

        public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
            // TODO Auto-generated method stub
            return null;
        }

        public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            // TODO Auto-generated method stub
            return null;
        }

        public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
            // TODO Auto-generated method stub
            return null;
        }

        public boolean getAutoCommit() throws SQLException {
            // TODO Auto-generated method stub
            return false;
        }

        public String getCatalog() throws SQLException {
            // TODO Auto-generated method stub
            return null;
        }

        public Properties getClientInfo() throws SQLException {
            // TODO Auto-generated method stub
            return null;
        }

        public String getClientInfo(String name) throws SQLException {
            // TODO Auto-generated method stub
            return null;
        }

        public int getHoldability() throws SQLException {
            // TODO Auto-generated method stub
            return 0;
        }

        public DatabaseMetaData getMetaData() throws SQLException {
            // TODO Auto-generated method stub
            return null;
        }

        public int getTransactionIsolation() throws SQLException {
            // TODO Auto-generated method stub
            return 0;
        }

        public Map<String, Class<?>> getTypeMap() throws SQLException {
            // TODO Auto-generated method stub
            return null;
        }

        public SQLWarning getWarnings() throws SQLException {
            // TODO Auto-generated method stub
            return null;
        }

        public boolean isClosed() throws SQLException {
            // TODO Auto-generated method stub
            return false;
        }

        public boolean isReadOnly() throws SQLException {
            // TODO Auto-generated method stub
            return false;
        }

        public boolean isValid(int timeout) throws SQLException {
            // TODO Auto-generated method stub
            return false;
        }

        public String nativeSQL(String sql) throws SQLException {
            // TODO Auto-generated method stub
            return null;
        }

        public CallableStatement prepareCall(String sql) throws SQLException {
            // TODO Auto-generated method stub
            return null;
        }

        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
            // TODO Auto-generated method stub
            return null;
        }

        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            // TODO Auto-generated method stub
            return null;
        }

        public PreparedStatement prepareStatement(String sql) throws SQLException {
            // TODO Auto-generated method stub
            return null;
        }

        public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
            // TODO Auto-generated method stub
            return null;
        }

        public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
            // TODO Auto-generated method stub
            return null;
        }

        public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
            // TODO Auto-generated method stub
            return null;
        }

        public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
            // TODO Auto-generated method stub
            return null;
        }

        public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            // TODO Auto-generated method stub
            return null;
        }

        public void releaseSavepoint(Savepoint savepoint) throws SQLException {
            // TODO Auto-generated method stub
        }

        public void rollback() throws SQLException {
            // TODO Auto-generated method stub
        }

        public void rollback(Savepoint savepoint) throws SQLException {
            // TODO Auto-generated method stub
        }

        public void setAutoCommit(boolean autoCommit) throws SQLException {
            // TODO Auto-generated method stub
        }

        public void setCatalog(String catalog) throws SQLException {
            // TODO Auto-generated method stub
        }

        public void setClientInfo(Properties properties) throws SQLClientInfoException {
            // TODO Auto-generated method stub
        }

        public void setClientInfo(String name, String value) throws SQLClientInfoException {
            // TODO Auto-generated method stub
        }

        public void setHoldability(int holdability) throws SQLException {
            // TODO Auto-generated method stub
        }

        public void setReadOnly(boolean readOnly) throws SQLException {
            // TODO Auto-generated method stub
        }

        public Savepoint setSavepoint() throws SQLException {
            // TODO Auto-generated method stub
            return null;
        }

        public Savepoint setSavepoint(String name) throws SQLException {
            // TODO Auto-generated method stub
            return null;
        }

        public void setTransactionIsolation(int level) throws SQLException {
            // TODO Auto-generated method stub
        }

        public void setTypeMap(Map<String, Class<?>> arg0) throws SQLException {
            // TODO Auto-generated method stub
        }

        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            // TODO Auto-generated method stub
            return false;
        }

        public <T> T unwrap(Class<T> iface) throws SQLException {
            return con.unwrap(iface);
        }

        @Override
        public void setSchema(String schema) throws SQLException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public String getSchema() throws SQLException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public void abort(Executor executor) throws SQLException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public int getNetworkTimeout() throws SQLException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
    }
}
