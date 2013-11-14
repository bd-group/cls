/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator.outputoperator;

/**
 *
 * @author lbh
 */
public class TableColumn {

    String name;
    String type;

    public TableColumn(String pName, String pType) {
        this.name = pName;
        this.type = pType;
    }
}
