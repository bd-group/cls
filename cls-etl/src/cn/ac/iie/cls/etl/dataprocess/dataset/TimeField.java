package cn.ac.iie.cls.etl.dataprocess.dataset;

public class TimeField extends Field<TimeField>
{
	String timeFieldValue;
	public TimeField(String fieldValue)
	{
		timeFieldValue = fieldValue;
		// TODO Auto-generated constructor stub
	}

	public String toString()
	{
		// TODO Auto-generated method stub
		if(timeFieldValue == null) {
			return null;
		} else {
			return timeFieldValue;
		}
	}

	@Override
	public int compareTo(TimeField anotherTimeField)
	{
		// TODO Auto-generated method stub
		return this.timeFieldValue.compareTo(anotherTimeField.timeFieldValue);
	}
	
	public String getString()
	{
		// TODO Auto-generated method stub
		if(timeFieldValue == null) {
			return null;
		} else {
			return timeFieldValue;
		}
	}

	@Override
	public Object getFieldValue()
	{
		// TODO Auto-generated method stub
		return timeFieldValue;
	}
}
