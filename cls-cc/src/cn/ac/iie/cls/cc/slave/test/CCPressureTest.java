/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.test;

import cn.ac.iie.cls.cc.util.XMLReader;
import java.io.ByteArrayInputStream;
import java.util.UUID;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.DefaultHttpClient;

/**
 *
 * @author root
 */
public class CCPressureTest {

    public static void main(String[] args) {
//        String dataProcessDescriptor = XMLReader.getXMLContent("f_917mt_incident-dp-spec.xml");
        PressureTest p1 = new PressureTest(XMLReader.getXMLContent("f_917mt_incident-dp-spec.xml"));
        PressureTest p2 = new PressureTest(XMLReader.getXMLContent("f_abflow-dp-spec.xml"));
        PressureTest p3 = new PressureTest(XMLReader.getXMLContent("f_917mt_mobile-dp-spec.xml"));
        PressureTest p4 = new PressureTest(XMLReader.getXMLContent("f_917mt_phishing-dp-spec.xml"));
        PressureTest p5 = new PressureTest(XMLReader.getXMLContent("f_917mt_webbackdoor-dp-spec.xml"));
        PressureTest p6 = new PressureTest(XMLReader.getXMLContent("f_917mt_special-dp-spec.xml"));
        p1.start();
        p2.start();
        p3.start();
        p4.start();
        p5.start();
        p6.start();
//        for (int i = 0; i < 100; i++) {
//            PressureTest p2 = new PressureTest(XMLReader.getXMLContent("f_abflow-dp-spec.xml"));
//            p2.start();
//        }
    }

    static class PressureTest extends Thread {

        String dataProcessDescriptor;

        public PressureTest(String pDataProcessDescriptor) {
            dataProcessDescriptor = pDataProcessDescriptor;
        }

        public void run() {
            HttpClient httpClient = new DefaultHttpClient();
//            for (int i = 0; i < 100; i++) {
            while(true){
                try {
                    HttpPost httppost = new HttpPost("http://10.128.125.74:7060/resources/dataetl/execute");
                    String content = dataProcessDescriptor.replace("3a000e", UUID.randomUUID().toString());
                    InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream(content.getBytes()), -1);
                    reqEntity.setContentType("binary/octet-stream");
                    reqEntity.setChunked(true);
                    httppost.setEntity(reqEntity);
                    HttpResponse response = httpClient.execute(httppost);
                    System.out.println(response.getStatusLine());
                    httppost.releaseConnection();
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
                
                try{
                    Thread.sleep(300);
                }catch(Exception ex){
                    
                }
            }
        }
    }
}
