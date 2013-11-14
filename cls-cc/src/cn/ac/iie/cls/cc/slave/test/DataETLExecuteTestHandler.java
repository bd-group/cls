/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.test;

import cn.ac.iie.cls.cc.slave.SlaveHandler;
import cn.ac.iie.cls.cc.slave.dataetl.DataETLExecuteHandler;
import cn.ac.iie.cls.cc.util.XMLReader;

/**
 *
 * @author alexmu
 */
public class DataETLExecuteTestHandler implements SlaveHandler {

    @Override
    public String execute(String pRequestContent) {
// 	for(int i = 0 ; i < 200; ++i){
//	    String dataProcessDesc = XMLReader.getXMLContent("try"+i+".xml");
//	    DataETLExecuteHandler dataETLExecuteHandler = new DataETLExecuteHandler();
//	    dataETLExecuteHandler.execute(dataProcessDesc);
//	}
 //       String dataProcessDesc = XMLReader.getXMLContent("f_mobile_sinkhole-dp-spec.xml");
//        String dataProcessDesc = XMLReader.getXMLContent("f_cymru_ddos_rsv2-dp-spec.xml");
//	String dataProcessDesc = XMLReader.getXMLContent("f_917mt_phishing-dp-spec.xml");
        String dataProcessDesc = XMLReader.getXMLContent("GlobalTableOuputOperator-test-specific.xml");
//        String dataProcessDesc = XMLReader.getXMLContent("f_917mt_webbackdoor-dp-spec.xml");
//        String dataProcessDesc = XMLReader.getXMLContent("f_917mt_ceshi-dp-spec.xml");
//        String dataProcessDesc = XMLReader.getXMLContent("f_abflow-dp-spec.xml");
//        String dataProcessDesc = XMLReader.getXMLContent("f_bgpmon_incident-dp-spec.xml");
//        String dataProcessDesc = XMLReader.getXMLContent("f_bgpmon_resource-dp-spec.xml");

        DataETLExecuteHandler dataETLExecuteHandler = new DataETLExecuteHandler();
        return dataETLExecuteHandler.execute(dataProcessDesc);

    }
}
