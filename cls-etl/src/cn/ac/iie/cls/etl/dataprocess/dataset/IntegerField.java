package cn.ac.iie.cls.etl.dataprocess.dataset;

public class IntegerField extends Field<IntegerField> {

    Integer integerValue;

    public IntegerField(Integer pFieldValue) {
        this.integerValue = pFieldValue;
        // TODO Auto-generated constructor stub
    }

    public String toString() {
        // TODO Auto-generated method stub
    	if(integerValue == null) {
    		return null;
    	} else {
    		return integerValue.toString();
    	}
    }

    @Override
    public int compareTo(IntegerField anotherIntegerField) {
        // TODO Auto-generated method stub
        return this.integerValue.compareTo(anotherIntegerField.integerValue);
    }

    public int getInt() {
        return integerValue.intValue();
    }

	@Override
	public Object getFieldValue()
	{
		// TODO Auto-generated method stub
		return integerValue;
	}
}
