package com.datamunging;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.datamunging.query.parser.AggregateFunction;
import com.datamunging.query.parser.Query;

public class GroupByQueryReader implements DataReader{

	DataRow dataRow;
	List<DataRow> result;
	Header header;
	int index;
	public GroupByQueryReader() {
		result=new ArrayList<>();
		header=new Header();
	}
	String fileRecord;
	@Override
	public List<DataRow> executeQuery(Query queryParameter) {
		
		int groupByColumnIndex;

		List<String> groupByColumnValueList = new ArrayList<>();

		groupByColumnIndex = header.getSingleColumnIndex(queryParameter.getGroupBy(),queryParameter.getFilePath().trim());
		try (BufferedReader bufferReader = new BufferedReader(new FileReader(queryParameter.getFilePath().trim())))
		{
			bufferReader.readLine();
			while ((fileRecord = bufferReader.readLine()) != null) {
				String record[] = fileRecord.split(",");
				groupByColumnValueList.add(record[groupByColumnIndex]);
			}
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		Set<String> groupByColumnSet = new LinkedHashSet<>();
		for (String value : groupByColumnValueList) {
			groupByColumnSet.add(value);

		}

		List<AggregateFunction> aggregateFunctionList = queryParameter.getAggregateFunctions();
		aggregateFunctionList.forEach(aggregateFunction -> 
		{
			try(BufferedReader bufferReader = new BufferedReader(new FileReader(queryParameter.getFilePath().trim())))
			{
				int sum;
				List<Integer> aggregateColumnValueList = new ArrayList<>();
				int aggregateColumnIndex = header.getSingleColumnIndex(aggregateFunction.getAggregateFunctionColumn(),queryParameter.getFilePath().trim());
				bufferReader.readLine();
				String record[];
				while ((fileRecord = bufferReader.readLine()) != null) {
					record = fileRecord.split(",");

					if (record[aggregateColumnIndex].isEmpty()) {
						aggregateColumnValueList.add(new Integer(0));
					} else {
						aggregateColumnValueList.add(Integer.parseInt(record[aggregateColumnIndex]));
					}
				}
				//create a class called utility and put the common code of aggregate function in the same class
				int min,max;
				min=aggregateColumnValueList.get(0);
				max=aggregateColumnValueList.get(0);
				String functionName = aggregateFunction.getFunctionName();
				for (String column : groupByColumnSet) 
				{
					int counter = 0;
					dataRow = new DataRow();
					dataRow.put(counter, column);

					switch (functionName) {
					case "sum":
						sum = 0;
						for (int i = 0; i < groupByColumnValueList.size(); i++) {
							if (column.trim().equals(groupByColumnValueList.get(i).trim())) {
								sum = sum + aggregateColumnValueList.get(i);
							}
						}

						dataRow.put(++counter, String.valueOf(sum));
						break;
					case "avg":
						int noOfColumn = 0;
						sum = 0;
						for (int i = 0; i < groupByColumnValueList.size(); i++) {
							if (column.trim().equals(groupByColumnValueList.get(i).trim())) {
								noOfColumn++;
								sum = sum + aggregateColumnValueList.get(i);
							}
						}
						float avg = (float) sum / noOfColumn;
						dataRow.put(++counter, String.valueOf(avg));
						break;
					case "min":
						for (int i = 0; i < groupByColumnValueList.size(); i++) {
							if (column.trim().equals(groupByColumnValueList.get(i).trim())) {
								if(aggregateColumnValueList.get(i)<min)
								{
									min=aggregateColumnValueList.get(i);
								}
							}
						}
						dataRow.put(++counter, String.valueOf(min));
						break;
					case "max":
						for (int i = 0; i < groupByColumnValueList.size(); i++) {
							if (column.trim().equals(groupByColumnValueList.get(i).trim())) {
								if(aggregateColumnValueList.get(i)>max)
								{
									max=aggregateColumnValueList.get(i);
								}
							}
						}
						dataRow.put(++counter, String.valueOf(max));
						break;
					case "count":
						int count=0;
						for (int i = 0; i < groupByColumnValueList.size(); i++) {
							if (column.trim().equals(groupByColumnValueList.get(i).trim())) {
								count++;
							}
						}

						dataRow.put(++counter, String.valueOf(count));
						break;

					}

					result.add(dataRow);
				}
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
			}
		});
		
		sortData(result, queryParameter);
		return result;
	}
	
	private boolean sortData(List<DataRow> data,Query queryParameters)
	{
		int indexOfOrderByColumn;
		String orderByColumn;
		orderByColumn=queryParameters.getOrderBy();
		if(queryParameters.getOrderBy()!=null)
		{
			indexOfOrderByColumn=header.getSingleColumnIndex(orderByColumn, queryParameters.getFilePath().trim());
			DataSorting dataSort=new DataSorting();
			dataSort.setSortingIndex(indexOfOrderByColumn);
			Collections.sort(data,dataSort);
			return true;
		}
		else
		{
			return false;
		}
	}
	

}
