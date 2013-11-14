package cn.ac.iie.cls.etl.dataprocess.dataset;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateField extends Field<DateField> {

    static SimpleDateFormat standardDateTimeFormat = new SimpleDateFormat("yyyyMMdd");
    Date dateValue;

    public DateField(Date pFieldValue) {
        dateValue = pFieldValue;
        // TODO Auto-generated constructor stub
    }

    public String toString() {
        // TODO Auto-generated method stub
        if (dateValue == null) {
            return null;
        } else {
            return standardDateTimeFormat.format(dateValue);
        }
    }

    @Override
    public int compareTo(DateField anotherDateField) {
        // TODO Auto-generated method stub
        return this.dateValue.compareTo(anotherDateField.dateValue);
    }

    public Date getDate() {
        return dateValue;
    }

    @Override
    public Object getFieldValue() {
        // TODO Auto-generated method stub
        return dateValue;
    }
}
