package cn.ac.iie.cls.etl.dataprocess.dataset;

public class FloatField extends Field<FloatField>
{
	Float floatValue;
	public FloatField(Float pFieldValue)
	{
		this.floatValue = pFieldValue;
		// TODO Auto-generated constructor stub
	}

	public String toString()
	{
		// TODO Auto-generated method stub
		if(floatValue == null) {
			return null;
		} else {
			return floatValue.toString();
		}
	}

	@Override
	public int compareTo(FloatField anotherFloatField)
	{
		// TODO Auto-generated method stub
		return this.floatValue.compareTo(anotherFloatField.floatValue);
	}

	public float getFloat() {
		return floatValue.floatValue();
	}

	@Override
	public Object getFieldValue()
	{
		// TODO Auto-generated method stub
		return floatValue;
	}
}
