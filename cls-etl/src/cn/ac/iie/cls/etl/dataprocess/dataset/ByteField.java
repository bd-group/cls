package cn.ac.iie.cls.etl.dataprocess.dataset;
/**
 * 
 * @author hanbing
 *
 */
public class ByteField extends Field<ByteField>
{
	Byte byteValue;
		
	public ByteField(Byte pFieldValue)
	{
		this.byteValue = pFieldValue;
		// TODO Auto-generated constructor stub
	}
	
	public String toString()
	{
		// TODO Auto-generated method stub
		if(byteValue == null) {
			return null;
		} else {
			return byteValue.toString();
		}
	}

	@Override
	public int compareTo(ByteField anotherByteField)
	{
		return this.byteValue.compareTo(anotherByteField.byteValue);
	}
	
	public byte getByte() {
		return byteValue.byteValue();
	}

	@Override
	public Object getFieldValue()
	{
		// TODO Auto-generated method stub
		return byteValue;
	}

}
