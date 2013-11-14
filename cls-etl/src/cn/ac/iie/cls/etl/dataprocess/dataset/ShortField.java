package cn.ac.iie.cls.etl.dataprocess.dataset;

public class ShortField extends Field<ShortField>
{
	Short shortValue;
	public ShortField(Short fieldValue)
	{
		shortValue = fieldValue;
		// TODO Auto-generated constructor stub
	}
	
	public String toString(Object obj)
	{
		// TODO Auto-generated method stub
		if(shortValue == null) {
			return null;
		} else {
			return shortValue.toString();
		}
	}

	@Override
	public int compareTo(ShortField anotherShortField)
	{
		// TODO Auto-generated method stub
		return this.shortValue.compareTo(anotherShortField.shortValue);
	}
	
	public short getShort() {
		return shortValue.shortValue();
	}

	@Override
	public Object getFieldValue()
	{
		// TODO Auto-generated method stub
		return shortValue;
	}

}
