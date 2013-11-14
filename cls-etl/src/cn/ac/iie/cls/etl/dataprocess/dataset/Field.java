package cn.ac.iie.cls.etl.dataprocess.dataset;

import java.io.Serializable;

import cn.ac.iie.cls.etl.dataprocess.operator.inputoperator.Column;

/**
 * 
 * @author hanbing
 *
 */
public abstract class Field<T> implements Comparable<T>,Serializable{
	/**
	 * 
	 * @return an Object type of the current field Value
	 */
	public abstract Object getFieldValue();
	
	//public abstract Field setFieldValue(Object objectValue);
}
