package com.hoob.flink.utils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author zhuqinhe
 */
public class ReflectIUtils {

	/**
	 * 根据属性，拿到set方法，并把值set到对象中
	 * @param obj 对象
	 * @param filedName 需要设置值得属性
	 * @param value
	 * @throws SecurityException
	 * @throws NoSuchFieldException
	 */
	public static void setValue(Object obj, String filedName, Object value) throws Exception {
		String methodName = "set" + filedName.substring(0, 1).toUpperCase() + filedName.substring(1);
		Class<?> typeClass = obj.getClass().getDeclaredField(filedName).getType();
		try {
			Method method = obj.getClass().getDeclaredMethod(methodName, new Class[] { typeClass });
			method.invoke(obj, new Object[] { getClassTypeValue(typeClass, value) });
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	/**
	 * @param <T>
	 * @category 利用反射机制通过属性名获取MongodbData对象的属性值
	 * @param fieldName  获取属性值的属性名
	 * @return
	 * @throws SecurityException

	 */
	public static Object getValue(String fieldName, Object obj) throws Exception {
		fieldName = fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);// 拼接获取属性get方法的名字
		Method m;
		m = obj.getClass().getMethod("get" + fieldName);// 获取get方法
		return m.invoke(obj);// 获取属性值
	}

	public static List<String> getFieldNames(Class<?> cls) {
		Field[] fields = cls.getDeclaredFields();
		List<String> filedNameList = new ArrayList<String>(fields.length);
		for (Field f : fields) {
			filedNameList.add(f.getName());
		}

		return filedNameList;
	}

	/**
	 * 通过class类型获取获取对应类型的值

	 * @param value
	 * @return Object
	 * @throws Exception
	 */
	public static byte[] getClassTypeValue(Object value) throws Exception {

		if (value == null) {
			return Bytes.toBytes(0);
		}
		Class<?> typeClass = value.getClass();
		if (typeClass == int.class || typeClass == Integer.class) {
			return Bytes.toBytes((Integer) value);
		} else if (typeClass == short.class || typeClass == Short.class) {
			return Bytes.toBytes((Short) value);
		} else if (typeClass == double.class || typeClass == Double.class) {
			return Bytes.toBytes((Double) value);
		} else if (typeClass == long.class || typeClass == Long.class) {
			return Bytes.toBytes((Long) value);
		} else if (typeClass == String.class) {
			return Bytes.toBytes((String) value);
		} else if (typeClass == boolean.class || typeClass == Boolean.class) {
			return Bytes.toBytes((Boolean) value);
		} else if (typeClass == BigDecimal.class) {
			return Bytes.toBytes((BigDecimal) value);
		} else if (typeClass == float.class || typeClass == Float.class) {
			return Bytes.toBytes((Float) value);
		} else {
			return Bytes.toBytes(0);
		}
	}

	/**
	 * 通过class类型获取获取对应类型的值
	 * @param typeClass
	 * @param value
	 * @return Object
	 * @throws Exception
	 */
	private static Object getClassTypeValue(Class<?> typeClass, Object value) throws Exception {
		if (typeClass == int.class || typeClass == Integer.class) {
			if (null == value) {
				return 0;
			}
			return Bytes.toInt((byte[]) value);
		} else if (typeClass == short.class || typeClass == Short.class) {
			if (null == value) {
				return 0;
			}
			return Bytes.toShort((byte[]) value);
		} else if (typeClass == byte.class) {
			if (null == value) {
				return 0;
			}
			return value;
		} else if (typeClass == double.class || typeClass == Double.class) {
			if (null == value) {
				return 0;
			}

			return Bytes.toDouble((byte[]) value);
		} else if (typeClass == long.class || typeClass == Long.class) {
			if (null == value) {
				return 0;
			}
			int size = ((byte[]) value).length;
			/*
			 * if (size < 8) { byte[] temp = new byte[8]; byte[] val = (byte[])
			 * value; for (int s = 0; s < val.length; s++) { temp[s] = val[s]; }
			 * value = temp; }
			 */
			/*
			 * if (size < 8) { byte[] temp = new byte[8]; byte[] val = (byte[])
			 * value; for (int s = 0; s < val.length; s++) { temp[s + 8 - size]
			 * = val[s]; } value = temp; }
			 */
			if (size == 4) {
				return (long) Bytes.toInt((byte[]) value);
			}
			return Bytes.toLong((byte[]) value);
		} else if (typeClass == String.class) {
			if (null == value) {
				return "";
			}
			return Bytes.toString((byte[]) value);
		} else if (typeClass == boolean.class || typeClass == Boolean.class) {
			if (null == value) {
				return false;
			}
			return Bytes.toBoolean((byte[]) value);
		} else if (typeClass == BigDecimal.class) {
			if (null == value) {
				return new BigDecimal(0);
			}
			return new BigDecimal(value + "");
		} else if (typeClass == float.class || typeClass == Float.class) {
			if (null == value) {
				return 0;
			}
			int size = ((byte[]) value).length;
			// 为八位时，先转成double，再转float
			if (size == 8) {
				double temResult = Bytes.toDouble((byte[]) value);
				return new Double(temResult).floatValue();
			}
			return Bytes.toFloat((byte[]) value);
		} else {
			return typeClass.cast(value);
		}
	}
}
