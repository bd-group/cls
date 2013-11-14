package cn.ac.iie.cls.etl.dataprocess.dataset;

public class StringField extends Field<StringField> {

    String stringValue;

    public StringField(String fieldValue) {
        stringValue = fieldValue;
        // TODO Auto-generated constructor stub
    }

    @Override
    public int compareTo(StringField anotherStringField) {
        // TODO Auto-generated method stub
        return this.stringValue.compareTo(anotherStringField.stringValue);
    }

    public String toString() {
        // TODO Auto-generated method stub
        if (stringValue != null) {
            return stringValue.toString();
        } else {
            return null;
        }
    }
    
    public String getString() {
        // TODO Auto-generated method stub
        if (stringValue != null) {
            return stringValue.toString();
        } else {
            return null;
        }
    }

	@Override
	public Object getFieldValue()
	{
		// TODO Auto-generated method stub
		return stringValue;
	}
}
