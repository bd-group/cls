package cn.ac.iie.cls.etl.dataprocess.dataset;

public class LongField extends Field<LongField>
{
	Long longValue;
	public LongField(Long fieldValue)
	{
		longValue = fieldValue;
		// TODO Auto-generated constructor stub
	}

	public String toString()
	{
		// TODO Auto-generated method stub
		if(longValue == null) {
			return null;
		} else {
			return longValue.toString();
		}
	}
	
	@Override
	public int compareTo(LongField anotherLongField)
	{
		// TODO Auto-generated method stub
		return longValue.compareTo(anotherLongField.longValue);
	}
        
    public long getLong(){
        return longValue.longValue();
    }

	@Override
	public Object getFieldValue()
	{
		// TODO Auto-generated method stub
		return longValue;
	}

}
