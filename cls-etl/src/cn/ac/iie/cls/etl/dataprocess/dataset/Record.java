/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.dataset;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author alexmu
 */
public class Record implements Serializable {

    DataSet dataSet = null;
    List<Field> fields = new ArrayList<Field>();

    public void appendField(Field pField) {
        fields.add(pField);
    }

    public List<Field> getAllFields() {
        return this.fields;
    }

    public Field getField(int pFieldIdx) {
        return fields.get(pFieldIdx);
    }

    public int size() {
        return fields.size();
    }

    public Field getField(String pFieldName) {
        return fields.get(dataSet.getFieldIdx(pFieldName));
    }

    public void setField(String pFieldName, Field pNewField) {
        fields.set(dataSet.getFieldIdx(pFieldName), pNewField);
    }

    //hanbing add
    public void removeField(int pFieldIdx) {
        fields.remove(pFieldIdx);
    }

    public Object copy(Record pRecord) throws IOException, ClassNotFoundException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(pRecord);
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()));
        return ois.readObject();
    }

    @Override
    public String toString() {
        return fields.toString();
    }
}
