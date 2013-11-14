package cn.ac.iie.cls.etl.dataprocess.dataset;

public class BooleanField extends Field<BooleanField>
{
	Boolean booleanValue;
	public BooleanField(Boolean fieldValue)
	{
		this.booleanValue = fieldValue;
		// TODO Auto-generated constructor stub
	}
	
	public String toString()
	{
		// TODO Auto-generated method stub
		if(booleanValue == null) {
			return null;
		} else {
			return this.booleanValue.toString();
		}
	}

	@Override
	public int compareTo(BooleanField anotherBooeanField)
	{
		// TODO Auto-generated method stub
		return this.booleanValue.compareTo(anotherBooeanField.booleanValue);
	}
	
	public boolean getBoolean() {
		return booleanValue.booleanValue();
	}

	@Override
	public Object getFieldValue()
	{
		// TODO Auto-generated method stub
		return booleanValue;
	}

}
