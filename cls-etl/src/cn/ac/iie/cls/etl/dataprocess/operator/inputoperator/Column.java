/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator.inputoperator;

import java.util.Comparator;

/**
 *
 * @author alexmu
 */
public class Column implements Comparable<Column> {

    public enum ColumnType {

        BYTE_TYPE, SHORT_TYPE, INTEGER_TYPE, LONG_TYPE, FLOAT_TYPE, DOUBLE_TYPE, DECIMAL_TYPE,
        BOOLEAN_TYPE, STRING_TYPE, DATE_TYPE, TIME_TYPE, TIMESTAMP_TYPE, IP_TYPE, MAC_TYPE,
        CURRENCY_TYPE, CLOB_TYPE, BLOB_TYPE, EXTEND_TYPE
    }
    String columnName;
    int columnIdx;
    String columnFormat;
    ColumnType columnType;

    public Column() {
    }

    public Column(String pColumnName, int pColumnIdx, ColumnType pColumnType, String pColumnFormat) {
        columnName = pColumnName;
        columnIdx = pColumnIdx;
        columnType = pColumnType;
        columnFormat = pColumnFormat;
    }

    public static ColumnType parseType(String pColumnType) throws Exception {
        pColumnType = pColumnType.toUpperCase();

        if (pColumnType.equals("BYTE")) {
            return ColumnType.BYTE_TYPE;
        } else if (pColumnType.equals("SHORT")) {
            return ColumnType.SHORT_TYPE;
        } else if (pColumnType.equals("INTEGER")) {
            return ColumnType.INTEGER_TYPE;
        } else if (pColumnType.equals("LONG")) {
            return ColumnType.LONG_TYPE;
        } else if (pColumnType.equals("FLOAT")) {
            return ColumnType.FLOAT_TYPE;
        } else if (pColumnType.equals("DOUBLE")) {
            return ColumnType.DOUBLE_TYPE;
        } else if (pColumnType.equals("DECIMAL")) {
            return ColumnType.DECIMAL_TYPE;
        } else if (pColumnType.equals("BOOLEAN")) {
            return ColumnType.BOOLEAN_TYPE;
        } else if (pColumnType.equals("STRING")) {
            return ColumnType.STRING_TYPE;
        } else if (pColumnType.equals("DATE")) {
            return ColumnType.DATE_TYPE;
        } else if (pColumnType.equals("TIME")) {
            return ColumnType.TIME_TYPE;
        } else if (pColumnType.equals("TIMESTAMP")) {
            return ColumnType.TIMESTAMP_TYPE;
        } else if (pColumnType.equals("IP")) {
            return ColumnType.IP_TYPE;
        } else if (pColumnType.equals("MAC")) {
            return ColumnType.MAC_TYPE;
        } else if (pColumnType.equals("CURRENCY")) {
            return ColumnType.CURRENCY_TYPE;
        } else if (pColumnType.equals("IP")) {
            return ColumnType.IP_TYPE;
        } else if (pColumnType.equals("CLOB")) {
            return ColumnType.CLOB_TYPE;
        } else if (pColumnType.equals("BLOB")) {
            return ColumnType.BLOB_TYPE;
        } else if (pColumnType.equals("EXTEND")) {
            return ColumnType.EXTEND_TYPE;
        } else {
            throw new Exception("not supported type " + pColumnType);
        }
    }

    @Override
    public int compareTo(Column anotherColumn) {
        if (this.columnIdx > anotherColumn.columnIdx) {
            return 1;
        } else if (this.columnIdx == anotherColumn.columnIdx) {
            return 0;
        } else {
            return -1;
        }
    }

    public String getColumnName() {
        return columnName;
    }

    public int getColumnIdx() {
        return columnIdx;
    }

    public String getColumnFormat() {
        return columnFormat;
    }

    public ColumnType getColumnType() {
        return columnType;
    }
}
