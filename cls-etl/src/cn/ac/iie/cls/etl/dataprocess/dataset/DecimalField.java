package cn.ac.iie.cls.etl.dataprocess.dataset;

public class DecimalField extends Field<DecimalField>
{
	String decimalFieldValue;
	public DecimalField(String fieldValue)
	{
		decimalFieldValue = fieldValue;
		// TODO Auto-generated constructor stub
	}
	
	public String toString()
	{
		// TODO Auto-generated method stub
		if(decimalFieldValue == null) {
			return null;
		} else {
			return decimalFieldValue.toString();
		}
	}

	@Override
	public int compareTo(DecimalField anotherDecimalField)
	{
		// TODO Auto-generated method stub
		return this.decimalFieldValue.compareTo(anotherDecimalField.decimalFieldValue);
	}
	
	public String getDecimalField() {
		if(decimalFieldValue == null) {
			return null;
		} else {
			return decimalFieldValue.toString();
		}
	}

	@Override
	public Object getFieldValue()
	{
		// TODO Auto-generated method stub
		return decimalFieldValue;
	}
}
