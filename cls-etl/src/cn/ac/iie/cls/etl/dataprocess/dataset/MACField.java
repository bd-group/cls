package cn.ac.iie.cls.etl.dataprocess.dataset;

public class MACField extends Field<MACField>
{
	String MACValue;
	public MACField(String pFieldValue)
	{
		this.MACValue = pFieldValue;
		// TODO Auto-generated constructor stub
	}
	
	public String toString()
	{
		// TODO Auto-generated method stub
		if(MACValue == null) {
			return null;
		} else {
			return MACValue.toString();
		}
	}

	@Override
	public int compareTo(MACField anotherMacField)
	{
		// TODO Auto-generated method stub
		return MACValue.compareTo(anotherMacField.MACValue);
	}
	
	public String getMAC()
	{
		// TODO Auto-generated method stub
		if(MACValue == null) {
			return null;
		} else {
			return MACValue.toString();
		}
	}

	@Override
	public Object getFieldValue()
	{
		// TODO Auto-generated method stub
		return MACValue;
	}

}
