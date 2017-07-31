package com.datamunging;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.datamunging.query.parser.AggregateFunction;
import com.datamunging.query.parser.Query;

public class AggregateQueryReader implements DataReader {

	DataRow dataRow;
	List<DataRow> result;
	Header header;
	int index;
	String fileRecord;
	String column;
	public AggregateQueryReader() {
		result=new ArrayList<>();
		header=new Header();
	}
	@Override
	public List<DataRow> executeQuery(Query queryParameter) {
		
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
				while ((fileRecord = bufferReader.readLine()) != null) {//do as multiple where clause
					record = fileRecord.split(",");

					if (record[aggregateColumnIndex].isEmpty()) {
						aggregateColumnValueList.add(new Integer(0));
					} else {
						aggregateColumnValueList.add(Integer.parseInt(record[aggregateColumnIndex]));
					}
				}
				
				int min,max;
				min=aggregateColumnValueList.get(0);
				max=aggregateColumnValueList.get(0);
				String functionName = aggregateFunction.getFunctionName();
				int counter = 0;
				dataRow = new DataRow();
				column=functionName+"("+aggregateFunction.getAggregateFunctionColumn()+")";
				switch (functionName) {
				case "sum":
					sum = 0;
					for (int i = 0; i < aggregateColumnValueList.size(); i++) {
						
							sum = sum + aggregateColumnValueList.get(i);
						
					}
					dataRow.put(++counter, column);
					dataRow.put(++counter, String.valueOf(sum));
					break;
				case "avg":
					int count = 0;
					sum = 0;
					for (int i = 0; i < aggregateColumnValueList.size(); i++) {
						
							count++;
							sum = sum + aggregateColumnValueList.get(i);
						
					}
					float avg = (float) sum / count;
					dataRow.put(++counter, column);
					dataRow.put(++counter, String.valueOf(avg));
					break;
				case "min":
					for (int i = 0; i < aggregateColumnValueList.size(); i++) {
						
							if(aggregateColumnValueList.get(i)<min)
							{
								min=aggregateColumnValueList.get(i);
							}
						
					}
					dataRow.put(++counter, column);
					dataRow.put(++counter, String.valueOf(min));
					break;
				case "max":
					for (int i = 0; i < aggregateColumnValueList.size(); i++) {
						
							if(aggregateColumnValueList.get(i)>max)
							{
								max=aggregateColumnValueList.get(i);
							}
						
					}
					dataRow.put(++counter, column);
					dataRow.put(++counter, String.valueOf(max));
					break;
				case "count":
					int total=0;
					for (int i = 0; i < aggregateColumnValueList.size(); i++) {
						
							total++;
						
					}
					dataRow.put(++counter, column);
					dataRow.put(++counter, String.valueOf(total));
					break;

				}

				result.add(dataRow);
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
			}
		});
		
		sortData(result, queryParameter);
		return result;
	}

	private boolean sortData(List<DataRow> data,Query queryParameter)
	{
		int indexOfOrderByColumn;
		String orderByColumn;
		orderByColumn=queryParameter.getOrderBy();
		if(queryParameter.getOrderBy()!=null)
		{
			indexOfOrderByColumn=header.getSingleColumnIndex(orderByColumn, queryParameter.getFilePath().trim());
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
