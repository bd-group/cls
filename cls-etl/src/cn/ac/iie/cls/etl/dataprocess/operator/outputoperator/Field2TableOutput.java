/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator.outputoperator;

/**
 *
 * @author lbh
 */
public class Field2TableOutput {

    String streamFieldName;
    String tableFieldName;

    public Field2TableOutput(String pStreamFieldName, String pTableFieldName) {
        streamFieldName = pStreamFieldName;
        tableFieldName = pTableFieldName;
    }
}
