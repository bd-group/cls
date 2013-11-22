/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.dataset;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.ac.iie.cls.etl.dataprocess.operator.inputoperator.Column;

import java.io.Serializable;

/**
 *
 * @author alexmu
 */
public class DataSet implements Serializable {

    public static final int VALID = 0;
    public static final int EOS = -1;
    private int status;
    private Map<String, Integer> fieldName2fieldIdx = new HashMap<String, Integer>();
    private Map<Integer, String> fieldIdx2fieldName = new HashMap<Integer, String>();
    private List<Record> records = new ArrayList<Record>();

    private DataSet(int pStatus) {
        status = pStatus;
    }

    public void appendRecord(Record pRecord) {
        this.records.add(pRecord);
        pRecord.dataSet = this;
    }

    public List<Record> getAllRecords() {
        return this.records;
    }

    public void removeRecord(int pRecordIdx) {
        this.records.remove(pRecordIdx);
    }

    public Record getRecord(int pRecordIdx) {
        return this.records.get(pRecordIdx);
    }

    public int size() {
        return this.records.size();
    }

    public int getStatus() {
        return status;
    }

    public boolean isValid() {
        return status == DataSet.VALID;
    }

    public int getFieldIdx(String pFieldName) {
        return fieldName2fieldIdx.get(pFieldName);
    }

    public String getFieldName(int pFieldIdx) {
        return fieldIdx2fieldName.get(pFieldIdx);
    }

    public void putFieldName2Idx(String pFieldName, int pFieldIdx) {
        fieldName2fieldIdx.put(pFieldName, pFieldIdx);
        fieldIdx2fieldName.put(pFieldIdx, pFieldName);
    }

    public int getFieldNum() {
        return fieldName2fieldIdx.size();
    }

    public List<String> getFieldNameList() {
        List<String> fieldNameList = new ArrayList<String>();
        fieldNameList.addAll(fieldIdx2fieldName.values());
        return fieldNameList;
    }

    public static DataSet getDataSet(List<Column> columnSet, int dataSetType) {
        DataSet dataSet = new DataSet(dataSetType);
        if (dataSetType == DataSet.VALID) {
            int idx = 0;
            for (Column column : columnSet) {
                dataSet.putFieldName2Idx(column.getColumnName(), idx++);
            }
        }
        return dataSet;
    }

    public boolean fieldNameExist(String pFieldName) {
        return fieldName2fieldIdx.get(pFieldName) == null ? false : true;
    }

    /**
     * @author hanbing to remove one field from current dataSet accronding to
     * the fieldName passed in
     * @param pFieldName
     */
    public void removeField(String pFieldName) {
        int fieldIndex = getFieldIdx(pFieldName);
        for (int i = 0; i < records.size(); i++) {
            records.get(i).removeField(fieldIndex);
        }
        fieldName2fieldIdx.remove(pFieldName);
        for (int i = fieldIndex; i < fieldName2fieldIdx.size(); i++) {
            String fieldName = fieldIdx2fieldName.get(i + 1);
            fieldName2fieldIdx.put(fieldName, i);
            fieldIdx2fieldName.put(i, fieldName);
        }
        fieldIdx2fieldName.remove(fieldIdx2fieldName.size() - 1);
    }

    /**
     * @author hanbing to get a cloned dataSet with the current dataSet's
     * metadatas
     * @return a cloned dataSet with the current dataSet's metadatas
     */
    public DataSet cloneDataSetWithMetadata() {
        DataSet dataSet = new DataSet(DataSet.VALID);
        List<String> fieldNames = this.getFieldNameList();
        for (int i = 0; i < fieldNames.size(); i++) {
            dataSet.fieldIdx2fieldName.put(i, fieldNames.get(i));
            dataSet.fieldName2fieldIdx.put(fieldNames.get(i), i);
        }
        return dataSet;
    }

    /**
     * @author hanbing
     * @return a two-dimensional Object array with datas in current dataSet
     */
    public Object[][] getDataSet() {
        Object[][] rows = new Object[records.size()][fieldName2fieldIdx.size()];
        for (int i = 0; i < records.size(); i++) {
            Record record = this.getRecord(i);
            for (int j = 0; j < record.size(); j++) {
                rows[i][j] = record.getField(j).getFieldValue();
            }
        }
        return rows;
    }

    /**
     * @author hanbing
     * @return a Record object created from a DataSet object
     */
    public Record getNewRecord() {
        return new Record();
    }
}
