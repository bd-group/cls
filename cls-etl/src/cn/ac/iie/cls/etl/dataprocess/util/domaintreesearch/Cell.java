/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.util.domaintreesearch;

/**
 *
 * @author root
 */
public class Cell {

    String value;
    boolean meanful;

    public Cell(String _value, boolean _meanful) {
        this.value = _value;
        this.meanful = _meanful;
    }
}
