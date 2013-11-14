package cn.ac.iie.cls.etl.dataprocess.dataset;

public class DoubleField extends Field<DoubleField>
{
	Double doubleValue;
	public DoubleField(Double fieldValue)
	{
		this.doubleValue = fieldValue;
		// TODO Auto-generated constructor stub
	}
	
	public String toString()
	{
		// TODO Auto-generated method stub
		if(doubleValue == null) {
			return null;
		} else {
			return doubleValue.toString();
		}
	}

	@Override
	public int compareTo(DoubleField anotherDoubleField)
	{
		// TODO Auto-generated method stub
		return this.doubleValue.compareTo(anotherDoubleField.doubleValue);
	}

	public double getDouble() {
		return doubleValue.doubleValue();
	}

	@Override
	public Object getFieldValue()
	{
		// TODO Auto-generated method stub
		return doubleValue;
	}
}
