package com.datamunging.test;

import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import com.datamunging.DataRow;
import com.datamunging.engine.QueryProcessingEngine;
import com.datamunging.query.parser.Query;
import com.datamunging.query.parser.QueryParser;

import static org.junit.Assert.*;

public class TestClass {

	QueryParser queryParser;
	QueryProcessingEngine queryProcessingEngine;

	String fileName;

	@Before
	public void init() {
		queryParser = new QueryParser();
		queryProcessingEngine = new QueryProcessingEngine();
		fileName = "D:\\DT Vsn3\\SampleData\\emp.csv";
	}

	
	@Test
	public void selectAllWhereTestCase() throws Exception {
		String query="select * from D:\\DT Vsn3\\SampleData\\emp.csv";
		Query queryParameter = queryParser.parseQuery(query);
		List<DataRow> dataSet = queryProcessingEngine.executeQuery(queryParameter);
		assertNotNull(dataSet);
		display(query, dataSet);

	}

	
	@Test
	public void selectAllWithoutWhereTestCase() throws Exception {
		String query="select EmpId,Name,Salary from D:\\DT Vsn3\\SampleData\\emp.csv";
		Query queryParameter = queryParser.parseQuery(query);
		List<DataRow> dataSet = queryProcessingEngine.executeQuery(queryParameter);
		assertNotNull(dataSet);
		display("Specific Columns", dataSet);

	}

	@Test
	public void selectColumnsWithoutWhereTestCase() throws Exception {
		String query="select * from D:\\DT Vsn3\\SampleData\\emp.csv where Salary>35000";
		Query queryParameters = queryParser
				.parseQuery(query);
		List<DataRow> dataSet = queryProcessingEngine.executeQuery(queryParameters);
		assertNotNull(dataSet);
		display(query, dataSet);

	}
	
	@Test
	public void withWhereGreaterThanTestCase() throws Exception {
		String query="select * from D:\\DT Vsn3\\SampleData\\emp.csv where Salary>35000 and Name=Anand";
		Query queryParameters = queryParser
				.parseQuery(query);
		List<DataRow> dataSet = queryProcessingEngine.executeQuery(queryParameters);
		assertNotNull(dataSet);
		display(query, dataSet);

	}

	
	@Test
	public void withWhereLessThanTestCase() throws Exception {
		String query="select * from D:\\DT Vsn3\\SampleData\\emp.csv order by Name";
		Query queryParameters = queryParser.parseQuery(query);
		List<DataRow> dataSet = queryProcessingEngine.executeQuery(queryParameters);
		assertNotNull(dataSet);
		display(query, dataSet);

	}

	
	@Test
	public void withWhereLessThanOrEqualToTestCase() throws Exception {
		String query="select Dept,sum(Salary) from D:\\DT Vsn3\\SampleData\\emp.csv";
		Query queryParameters = queryParser.parseQuery(query);
		List<DataRow> dataSet = queryProcessingEngine.executeQuery(queryParameters);
		assertNotNull(dataSet);
		display(query, dataSet);

	}

	@Test
	public void withWhereLessToTestCase() throws Exception {
		String query="select sum(Salary),count(Salary) from D:\\DT Vsn3\\SampleData\\emp.csv";
		Query queryParameters = queryParser.parseQuery(query);
		List<DataRow> dataSet = queryProcessingEngine.executeQuery(queryParameters);
		assertNotNull(dataSet);
		display(query, dataSet);

	}
	
	@Test
	public void withWhereGreaterThanOrEqualToTestCase() throws Exception {
		String query="select Dept,sum(Salary) from D:\\DT Vsn3\\SampleData\\emp.csv group by Dept";
		Query queryParameters = queryParser
				.parseQuery(query);
		List<DataRow> dataSet = queryProcessingEngine.executeQuery(queryParameters);
		assertNotNull(dataSet);
		display(query, dataSet);

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
