/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator.inputoperator;

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
      //  pColumnType = pColumnType.toUpperCase();

        if (pColumnType.equals("Byte")) {
            return ColumnType.BYTE_TYPE;
        } else if (pColumnType.equals("Short")) {
            return ColumnType.SHORT_TYPE;
        } else if (pColumnType.equals("Integer")) {
            return ColumnType.INTEGER_TYPE;
        } else if (pColumnType.equals("Long")) {
            return ColumnType.LONG_TYPE;
        } else if (pColumnType.equals("Float")) {
            return ColumnType.FLOAT_TYPE;
        } else if (pColumnType.equals("Double")) {
            return ColumnType.DOUBLE_TYPE;
        } else if (pColumnType.equals("Decimal")) {
            return ColumnType.DECIMAL_TYPE;
        } else if (pColumnType.equals("Boolean")) {
            return ColumnType.BOOLEAN_TYPE;
        } else if (pColumnType.equals("String")) {
            return ColumnType.STRING_TYPE;
        } else if (pColumnType.equals("Date")) {
            return ColumnType.DATE_TYPE;
        } else if (pColumnType.equals("Time")) {
            return ColumnType.TIME_TYPE;
        } else if (pColumnType.equals("Timestamp")) {
            return ColumnType.TIMESTAMP_TYPE;
        } else if (pColumnType.equals("Ip")) {
            return ColumnType.IP_TYPE;
        } else if (pColumnType.equals("Mac")) {
            return ColumnType.MAC_TYPE;
        } else if (pColumnType.equals("Currency")) {
            return ColumnType.CURRENCY_TYPE;
        } else if (pColumnType.equals("Clob")) {
            return ColumnType.CLOB_TYPE;
        } else if (pColumnType.equals("Blob")) {
            return ColumnType.BLOB_TYPE;
        } else if (pColumnType.equals("Extend")) {
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
