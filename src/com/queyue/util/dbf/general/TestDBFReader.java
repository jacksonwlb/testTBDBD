package com.queyue.util.dbf.general;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.tinygroup.dbf.DbfReader;
import org.tinygroup.dbf.Reader;

import com.alibaba.fastjson.JSON;

public class TestDBFReader {

	List<T_TDD> tddList;
	
	String destDBF = "E:\\PROJECT\\OKAO\\data\\录取库\\11_北京\\本科一批（理工）_39\\t_tdd.dbf";
	
	@Before
	public void setUp() throws Exception {
		tddList = new ArrayList<>();
		map = new HashMap<>();
	}

	@After
	public void tearDown() throws Exception {
		tddList = null;
		map = null;
	}
	
	private String msg()
	{
		return "size:"+tddList.size()+",json:"+JSON.toJSONString(tddList);
	}

	private void checkStatus()
	{
		if(tddList.size()!=39)fail();
		
		for (T_TDD t_TDD : tddList) {
			if(t_TDD==null)fail();
			if(t_TDD.ksh==null){fail();return;}
			if(t_TDD.ksh.isEmpty())fail();
		}
	}
	
	private void checkStatus2()
	{
		if(map.size()!=39)fail();
		
		for (T_TDD t_TDD : map.values()) {
			if(t_TDD==null)fail();
			if(t_TDD.ksh==null){fail();return;}
			if(t_TDD.ksh.isEmpty())fail();
		}
	}


	@Test
	public void test1()
	{
		try {
			Reader dbfReader = DbfReader.parse(destDBF);
			
			while (dbfReader.hasNext()) {
				
				dbfReader.next();
				
				T_TDD t = new T_TDD();
				dbfReader.parseRecord(t, dbfReader.getFields());
				tddList.add(t);
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			fail();
		}
		checkStatus();
		System.out.println("test1,"+msg());
	}
	
	@Test
	public void test2()
	{
		try {
			Reader dbfReader = DbfReader.parse(destDBF);
			
			while (dbfReader.hasNext()) {
				
				dbfReader.next();
				
				T_TDD t = dbfReader.parseRecord(T_TDD.class, dbfReader.getFields());
				tddList.add(t);
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			fail();
		}
		
		System.out.println("test2,"+msg());
		checkStatus();
	}

	@Test
	public void test3()
	{
		try {
			Reader dbfReader = DbfReader.parse(destDBF);
			
			while (dbfReader.hasNext()) {
				
				T_TDD t = dbfReader.parseNextRecord(T_TDD.class);
				tddList.add(t);
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			fail();
		}
		
		System.out.println("test3,"+msg());
		checkStatus();
	}
	
	@Test
	public void test4()
	{
		try {
			Reader dbfReader = DbfReader.parse(destDBF);
						
			tddList = dbfReader.parseRecordList(T_TDD.class);
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			fail();
		}

		System.out.println("test4,"+msg());
		checkStatus();
	}
	
	Map<String, T_TDD> map;
	@Test
	public void test5()
	{
		
		
		try {
			Reader dbfReader = DbfReader.parse(destDBF);
			
		
				
				map = dbfReader.parseRecordMap(T_TDD.class,"ksh");
				System.out.println("test5,"+"size:"+map.size()+",json:"+JSON.toJSONString(map));
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			fail();
		}

	
		checkStatus2();
	}
}
