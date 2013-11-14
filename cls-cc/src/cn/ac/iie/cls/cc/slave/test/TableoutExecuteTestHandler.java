/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.test;

import cn.ac.iie.cls.cc.slave.SlaveHandler;
import cn.ac.iie.cls.cc.slave.nosqlcluster.NoSqlClusterTableOutputHandler;
import cn.ac.iie.cls.cc.util.XMLReader;

/**
 *
 * @author root
 */
public class TableoutExecuteTestHandler implements SlaveHandler{
     

    @Override
    public String execute(String pRequestContent) {
        String dataProcessDesc = XMLReader.getXMLContent("tableOutputOperator-test-specific.xml");
        

        NoSqlClusterTableOutputHandler dataETLExecuteHandler = new NoSqlClusterTableOutputHandler();
        return dataETLExecuteHandler.execute(dataProcessDesc);
    }
}
