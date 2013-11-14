package cn.ac.iie.cls.etl.dataprocess.dataset;

import java.net.InetAddress;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import cn.ac.iie.cls.etl.dataprocess.operator.inputoperator.Column;

public class FieldFactory {

    public static Field getField(String pFieldValue, Column pColumn) throws Exception {
        if (pFieldValue == null) {
            return null;
        }
        pFieldValue=pFieldValue.trim();
        switch (pColumn.getColumnType()) {
            case BYTE_TYPE:
                return new ByteField(Byte.valueOf(pFieldValue));
            case SHORT_TYPE:
                return new ShortField(Short.valueOf(pFieldValue));
            case INTEGER_TYPE:
                return new IntegerField(Integer.valueOf(pFieldValue));
            case LONG_TYPE:               
                    return new LongField(Long.valueOf(pFieldValue));                
            case FLOAT_TYPE:
                return new FloatField(Float.valueOf(pFieldValue));
            case DOUBLE_TYPE:
                return new DoubleField(Double.valueOf(pFieldValue));
            case BOOLEAN_TYPE:
                return new BooleanField(Boolean.valueOf(pFieldValue));
            case STRING_TYPE:
                return new StringField(pFieldValue);
            case DATE_TYPE:
                DateFormat format = new SimpleDateFormat(pColumn.getColumnFormat());
                return new DateField(format.parse(pFieldValue));
            case TIME_TYPE:
                return new TimeField(pFieldValue);
            case TIMESTAMP_TYPE:
                DateFormat tsformat = new SimpleDateFormat(pColumn.getColumnFormat());
                return new TimestampField(tsformat.parse(pFieldValue));
            case IP_TYPE:
                InetAddress ia = InetAddress.getByName(pFieldValue.trim());
                return new IPField(ia);
            case MAC_TYPE:
                return new MACField(pFieldValue);
            default:
                return null;
        }
    }
}
