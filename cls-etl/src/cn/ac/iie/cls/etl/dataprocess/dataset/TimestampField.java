package cn.ac.iie.cls.etl.dataprocess.dataset;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TimestampField extends Field<TimestampField> {

    static SimpleDateFormat standardTimestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Date timestampValue;

    public TimestampField(Date pFieldValue) {
        this.timestampValue = pFieldValue;
        // TODO Auto-generated constructor stub
    }
    
    public TimestampField(String pFieldValue){
        this.timestampValue=new Date(pFieldValue);
        
    }
    

    public String toString() {
        // TODO Auto-generated method stub
        if (timestampValue == null) {
            return null;
        } else {
            return standardTimestampFormat.format(timestampValue);
        }
    }

    @Override
    public int compareTo(TimestampField anotherTimestampField) {
        // TODO Auto-generated method stub
        return this.timestampValue.compareTo(anotherTimestampField.timestampValue);
    }

    public Date getTimestampValue() {
        return timestampValue;
    }

    public String getString() {
        // TODO Auto-generated method stub
        if (timestampValue == null) {
            return null;
        } else {
            return this.timestampValue.toString();
        }
    }

    @Override
    public Object getFieldValue() {
        // TODO Auto-generated method stub
        return timestampValue;
    }
}
