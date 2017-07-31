package com.datamunging;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.datamunging.query.parser.Criteria;
import com.datamunging.query.parser.Query;

public class SimpleQueryReader implements DataReader{

	DataRow dataRow;
	List<DataRow> result;
	Header header;
	int index;
	Query queryParameter;
	public SimpleQueryReader() {
		
		result=new ArrayList<>();
		header=new Header();
	}
	@Override
	public List<DataRow> executeQuery(Query queryParameter) {
		this.queryParameter=queryParameter;
		result=getData(queryParameter);
		sortData(result, queryParameter);
		return result;
		
	}
	private List<DataRow> getData(Query queryParameter) {
	
		String fileRecord;
		
		List<Integer> headerColumnIndex=header.getSelectedColumnIndex(queryParameter);
		try(BufferedReader bufferReader = new BufferedReader(new FileReader(queryParameter.getFilePath().trim())))
		{
			bufferReader.readLine();
			if(queryParameter.isWhereCondition())
			{
				result=getDataFromWhereCondition(queryParameter);//pass the reader object so that it will not be opened twice
			}
			else
			{
				//put in another method
				while ((fileRecord = bufferReader.readLine()) != null) {
					index = 0;
					dataRow = new DataRow();
					String record[] = fileRecord.split(",");
					headerColumnIndex.forEach(colindex ->
					{
						if(record[colindex].isEmpty())
						{
							dataRow.put(index,"null");
						}
						else
						{
							dataRow.put(index, record[colindex]);
						}
						index++;
					});
					result.add(dataRow);
				}
			}
			
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		
		return result;
	}
	private List<DataRow> getDataFromWhereCondition(Query queryParameter)
	{
		String fileRecord;
		
		try(BufferedReader bufferReader = new BufferedReader(new FileReader(queryParameter.getFilePath().trim())))
		{
			bufferReader.readLine();
			while((fileRecord=bufferReader.readLine())!=null)
			{
				index=0;
				dataRow = new DataRow();
				String record[] = fileRecord.split(",");
				if(getDataWhereEachRow(record,queryParameter))
				{
					for(int i=0;i<record.length;i++)
					{
						dataRow.put(index, record[i]);
						index++;
					}
				}
				result.add(dataRow);	
			}
			
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		return result;
	}
	private boolean getDataWhereEachRow(String[] record,Query queryParameter) {
		List<Criteria> criteriaList=queryParameter.getRestrictions();
		List<String> logicalOperatorList=queryParameter.getLogicalOperators();
		boolean flag=false;
		int i=0;
		Criteria criteria=criteriaList.get(i);
		flag=getDataAfterApplyingCriteria(record,criteria);
		for(String operator:logicalOperatorList)
		{
			switch(operator)
			{
			case "and":
				criteria=criteriaList.get(++i);
				flag=flag&&getDataAfterApplyingCriteria(record,criteria);
				break;
			case "or":
				criteria=criteriaList.get(++i);
				flag=flag||getDataAfterApplyingCriteria(record, criteria);
				break;
			}
		}
		return flag;
	}
	private boolean getDataAfterApplyingCriteria(String[] record, Criteria criteria) {
		boolean flag=false;
		String operator=criteria.getOperator();
		String searchValue=criteria.getValue();
		List<Integer> headerColumnIndex=header.getSelectedColumnIndex(queryParameter);
		int whereIndex=header.getSingleColumnIndex(criteria.getColumnName(),queryParameter.getFilePath().trim());
		String dataValue=record[headerColumnIndex.get(whereIndex)];
		switch(operator)
		{
		case "=":
			if(dataValue.equals(searchValue))
			{
				flag=true;
			}
			break;
		case ">":
			if(Integer.parseInt(dataValue)>Integer.parseInt(searchValue))
			{
				flag=true;
			}
			break;
		case "<":
			if(Integer.parseInt(dataValue)<Integer.parseInt(searchValue))
			{
				flag=true;
			}
			break;
		case "<=":
			if(Integer.parseInt(dataValue)<=Integer.parseInt(searchValue))
			{
				flag=true;
			}
			break;
		case ">=":
			if(Integer.parseInt(dataValue)>=Integer.parseInt(searchValue))
			{
				flag=true;
			}
			break;
		case "!=":
			if(Integer.parseInt(dataValue)!=Integer.parseInt(searchValue))
			{
				flag=true;
			}
			break;
		}
		return flag;
	}
	private void sortData(List<DataRow> data,Query queryParameter)
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
			
		}
		
	}
}
