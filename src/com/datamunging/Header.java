package com.datamunging;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.datamunging.query.parser.Query;
import com.datamunging.query.parser.QueryParser;

public class Header {

	int i;
	Map<String, Integer> headerInfo;
	Map<String, Integer> queryColumnWithIndex;
	public Map<String, Integer> getHeaderInfo(String filePath) {
	
		headerInfo= new LinkedHashMap<>();
		i=0;
		try (BufferedReader reader = new BufferedReader(new FileReader(filePath)))
		{
			List<String> header=Arrays.asList(reader.readLine().split(","));
			
			header.forEach(value ->
			{
				headerInfo.put(value, i);
				i++;
			});
			
		
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		return headerInfo;
	
	}
	
	
	public Map<String, Integer> getQueryColumnWithIndex() {
		return queryColumnWithIndex;
	}


	public List<Integer> getSelectedColumnIndex(Query queryParameter)
	{
		queryColumnWithIndex=new LinkedHashMap<>();
		if(queryParameter.getFields()!=null)
		{
			Map<String,Integer> headerInfo=getHeaderInfo(queryParameter.getFilePath());
			List<String> header=getHeaderColumns(queryParameter.getFilePath());
			List<String> fields=queryParameter.getFields();
			List<Integer> fieldIndex=new ArrayList<>();
			fields.forEach(field ->
			{
				if(field.trim().equals("*"))
				{
					header.forEach(head -> 
					{
						fieldIndex.add(headerInfo.get(head.trim()));
						queryColumnWithIndex.put(head.trim(), headerInfo.get(head.trim()));
					});
				}
				else
				{
					queryColumnWithIndex.put(field.trim(), headerInfo.get(field));
					fieldIndex.add(headerInfo.get(field));
				}
			});
			
			return fieldIndex;
		}
		else
		{
			return null;
		}
	}

	private List<String> getHeaderColumns(String filePath)
	{
		try (BufferedReader reader = new BufferedReader(new FileReader(filePath)))
		{
			List<String> header=Arrays.asList(reader.readLine().split(","));
			return header;
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public int getSingleColumnIndex(String columnName,String filePath)
	{
		Map<String,Integer> headerInfo=getHeaderInfo(filePath);
		int columnIndex=headerInfo.get(columnName.trim());
		return columnIndex;
		
	}
}
