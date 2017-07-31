package com.datamunging.test;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.datamunging.DataRow;
import com.datamunging.engine.QueryProcessingEngine;
import com.datamunging.query.parser.Query;
import com.datamunging.query.parser.QueryParser;

public class DataMungingTest {
	
	QueryParser queryParser;
	QueryProcessingEngine queryProcessingEngine;
		
	String fileName;
	
		@Before
		public void init() {
			queryParser=new QueryParser();
			queryProcessingEngine=new QueryProcessingEngine();
			fileName="D:\\DT Vsn3\\SampleData\\emp.csv";
		}
		@Ignore
		@Test
		public void queryParserTest()  {
			Query queryParameter = queryParser.parseQuery("select Name,Salary from "+fileName+" order by Salary");

			System.out.println(queryParameter.getFields());
			System.out.println(queryParameter.getQueryType());	
			System.out.println(queryParameter.getOrderBy());
			System.out.println(queryParameter.getRestrictions().get(0).getColumnName()+"  "+queryParameter.getRestrictions().get(1).getColumnName());
			
		}
		
		@Test
		public void queryReaderTest()  {
			
			Query queryParameter=queryParser.parseQuery("select * from "+fileName+" where Salary>35000 and Name=Anand");
			List<DataRow> resultSet=queryProcessingEngine.executeQuery(queryParameter);	
			assertNotNull(resultSet);
			display("QueryReaderTest", resultSet);

		}
		
		private void display(String testCaseName, List<DataRow> result) {
			System.out.println(testCaseName);
			System.out.println("******************************************");

			for (Map<Integer, String> dataRow : result) {
				if (!dataRow.isEmpty()) {
					for (Map.Entry<Integer, String> data : dataRow.entrySet()) {
						System.out.print("  " + data.getValue());
					}
					System.out.println();
				}
			}
			System.out.println("******************************************");
			
			
		}


}
