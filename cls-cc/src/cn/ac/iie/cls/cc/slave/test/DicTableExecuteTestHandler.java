/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.test;

import cn.ac.iie.cls.cc.slave.SlaveHandler;
import cn.ac.iie.cls.cc.slave.nosqlcluster.NoSqlClusterDicTableSyncHandler;
import cn.ac.iie.cls.cc.util.XMLReader;

/**
 *
 * @author root
 */
public class DicTableExecuteTestHandler implements SlaveHandler{
     @Override
    public String execute(String pRequestContent) {
        String dataProcessDesc = XMLReader.getXMLContent("DicTableOperator-test-specific.xml");
        

        NoSqlClusterDicTableSyncHandler dataETLExecuteHandler = new NoSqlClusterDicTableSyncHandler();
        return dataETLExecuteHandler.execute(dataProcessDesc);
    }
}
