package com.excellence.dbf.javadbf;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;


public class DBFJdbcOdbcTest {
	public static void main(String[] args) {
		Connection conn = null;
		PreparedStatement pstm = null;
		ResultSet rs = null;
		
		//下面的代码其实都是基本的jdbc代码，所以编写上面基本没什么问题
		String DB_URL =
				"jdbc:odbc:Driver={Microsoft FoxPro VFP Driver (*.dbf)};" + 		//写法相对固定
				"UID=;"+
				"Deleted=Yes;"+
				"Null=Yes;"+
				"Collate=Machine;"+
				"BackgroundFetch=Yes;"+
				"Exclusive=No;"+
				"SourceType=DBF;"+     							//此处指定解析文件的后缀
				"SourceDB=E:\\users\\pengsy\\DBF\\data\\main"; 	//此处为dbf文件所在的目录
		
		try {
			Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");

			try {
				conn = DriverManager.getConnection(DB_URL);

				pstm = conn.prepareStatement("select * from XXB"); // 此处的XXB 为DBF数据文件的名字

				rs = pstm.executeQuery();
				ResultSetMetaData metaData = pstm.getMetaData();
				
				//展示dbf元数据信息
				System.out.println("metaData.getColumnCount():"+metaData.getColumnCount());
				System.out.println("**************************");
				for(int i = 1 ; i <= metaData.getColumnCount() ; i++){
					System.out.println("metaData.getCatalogName:"+metaData.getCatalogName(i));
					System.out.println("metaData.getColumnClassName:"+metaData.getColumnClassName(i));
					System.out.println("metaData.getColumnDisplaySize:"+metaData.getColumnDisplaySize(i));
					System.out.println("metaData.getColumnLabel:"+metaData.getColumnLabel(i));
					System.out.println("metaData.getColumnName:"+metaData.getColumnName(i));
					System.out.println("metaData.getColumnType:"+metaData.getColumnType(i));
					System.out.println("metaData.getColumnTypeName:"+metaData.getColumnTypeName(i));
					System.out.println("metaData.getPrecision:"+metaData.getPrecision(i));
					System.out.println("metaData.getScale:"+metaData.getScale(i));
					System.out.println("metaData.getSchemaName:"+metaData.getSchemaName(i));
					System.out.println("metaData.getTableName:"+metaData.getTableName(i));
					
					System.out.println("**************************");
				}
				
				//展示dbf中的行数据
				while(rs.next()){
					for(int i = 1 ; i <= metaData.getColumnCount() ; i++){
						System.out.println(rs.getString(i));
					}
					System.out.println("*******************************************");
				}

			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}finally{
				if(rs != null){
					rs.close();
				}
				if(pstm != null){
					pstm.close();
				}
				if(conn != null){
					conn.close();
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}