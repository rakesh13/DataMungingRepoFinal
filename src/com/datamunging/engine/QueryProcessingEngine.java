package com.datamunging.engine;

import java.util.ArrayList;
import java.util.List;

import com.datamunging.AggregateQueryReader;
import com.datamunging.DataReader;
import com.datamunging.DataRow;
import com.datamunging.GroupByQueryReader;
import com.datamunging.SimpleQueryReader;
import com.datamunging.query.parser.Query;

public class QueryProcessingEngine {

	DataReader dataReader;
	
	public QueryProcessingEngine() {
		// TODO Auto-generated constructor stub
		
	}
	
	public List<DataRow> executeQuery(Query queryParameter)
	{
		//List<DataRow> result=new ArrayList<>();
		try
		{
			String queryType=queryParameter.getQueryType();
			switch(queryType)
			{
			case "SIMPLE_QUERY":
				dataReader=new SimpleQueryReader();
				break;
			case "AGGREGATE_QUERY":
				dataReader=new AggregateQueryReader();
				break;
			case "GROUPBY_QUERY":
				dataReader=new GroupByQueryReader();
				break;
			}
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		return dataReader.executeQuery(queryParameter);
	}
}
