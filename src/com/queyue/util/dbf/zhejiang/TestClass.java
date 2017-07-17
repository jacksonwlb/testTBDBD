package com.queyue.util.dbf.zhejiang;

import java.lang.reflect.Field;

public class TestClass {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		T_TDD tdd = new T_TDD();
		
		Field fields[] = tdd.getClass().getDeclaredFields();
		System.out.println("=========");
		for (Field field : fields) {
			
			System.out.println(field.getName());
		}
		System.out.println("=========");
		try {
			Field field =
			T_TDD.class.getDeclaredField("KSH");
			field.set(tdd, "2011100155");
			System.out.println(tdd.ksh);
		} catch (NoSuchFieldException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("done");
	}

}
