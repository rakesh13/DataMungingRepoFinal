package com.datamunging.query.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class QueryParser {

	Query queryParameter;

	public QueryParser() {
		queryParameter = new Query();
	}

	public Query parseQuery(String queryString) {

		String splitQuery[];
		queryParameter.setQueryType("SIMPLE_QUERY");
		if (queryString.contains("where")) {
			
			splitQuery = queryString.split("where");
			
			queryString=splitQuery[0];
			queryParameter = this.whereWithOrderBy(splitQuery[1].trim());

		}
		
		
		queryParameter = this.selectwithOrderByClause(queryString.trim());
	
		return queryParameter;

	}

	private Query selectwithOrderByClause(String queryString) {

		String splitQuery[];
		String orderbyString = null;
		if (queryString.contains("order by")) {
			splitQuery = queryString.split("order by");
			orderbyString = this.orderbyClause(splitQuery[1]);
			queryParameter.setOrderBy(orderbyString);
			return this.selectWithGroupBy(splitQuery[0]);

		}
		else{
			

			queryParameter = this.selectWithGroupBy(queryString);
		}
		
		return queryParameter;
	}

	private Query selectWithGroupBy(String queryString) {
		// TODO Auto-generated method stub

		String splitQuery[];
		String groupbyString = null;
		if (queryString.contains("group by")) {
			splitQuery = queryString.split("group by");
			groupbyString = this.groupbyClause(splitQuery[1]);
			queryParameter.setGroupBy(groupbyString);
			queryParameter.setQueryType("GROUPBY_QUERY");
			return this.onlySelectClause(splitQuery[0]);

		} 
		else
		{
			queryParameter = this.onlySelectClause(queryString);
		}
		
		return queryParameter;
	}

	private Query onlySelectClause(String queryString) {

		List<String> fieldlist = new ArrayList<>();
		Pattern pattern = Pattern.compile("select(.*)from(.*)");
		List<AggregateFunction> aggregateFunctionlist=new ArrayList<>();

		String filename = null;
		Matcher matcher = pattern.matcher(queryString);
		if (matcher.find()) {
			if (matcher.group(1).contains("*")) {
				
				queryParameter.setAllColumns(true);
				fieldlist.add(matcher.group(1));
				
			} else {
				
				String[] fielditem = matcher.group(1).split(",");
				
				for (String field : fielditem) {
					if (field.contains("sum") || field.contains("avg") || field.contains("min")
							|| field.contains("max") || field.contains("count")) {
						
						AggregateFunction aggregateFunction=new AggregateFunction();
						aggregateFunction.setFunctionName(field.split("\\(")[0].trim());
						aggregateFunction.setAggregateFunctionColumn(field.split("\\(")[1].trim().split("\\)")[0].trim());
						aggregateFunctionlist.add(aggregateFunction);
						if(!(queryParameter.getQueryType().equals("GROUPBY_QUERY")))
						{
							queryParameter.setQueryType("AGGREGATE_QUERY");
						}
				}
					else
					{
						fieldlist.add(field.trim());
					}
					}
				if(!((queryParameter.getQueryType().equals("GROUPBY_QUERY")))&&!(queryParameter.getQueryType().equals("AGGREGATE_QUERY")))
				{
					queryParameter.setQueryType("SIMPLE_QUERY");
				}
				
				
				}
			}

			filename = matcher.group(2);
		
			queryParameter.setFields(fieldlist);
			queryParameter.setFilePath(filename.trim());
			queryParameter.setAggregateFunctions(aggregateFunctionlist);
			return queryParameter;

	}

	private Query whereWithOrderBy(String queryString) {
		String splitQuery[];
		String orderbyString = null;
		if (queryString.contains("order by")) {
			splitQuery = queryString.split("order by");
			orderbyString = this.orderbyClause(splitQuery[1]);
			queryParameter.setOrderBy(orderbyString);	
			return this.whereWithGroupBy(splitQuery[0]);

		} else {
			
					
			queryParameter = this.whereWithGroupBy(queryString);

		}
		return queryParameter;
	}

	private Query whereWithGroupBy(String queryString) {
		// TODO Auto-generated method stub

		String splitQuery[];
		String groupbyString = null;
		if (queryString.contains("group by")) {
			splitQuery = queryString.split("group by");
			groupbyString = this.groupbyClause(splitQuery[1]);
			queryParameter.setGroupBy(groupbyString);
			queryParameter.setQueryType("GROUPBY_QUERY");
			return this.onlyWhereClause(splitQuery[0]);

		} else {
			queryParameter = this.onlyWhereClause(queryString);
			queryParameter.setWhereCondition(true);
		}
		
		return queryParameter;
	}

	private Query onlyWhereClause(String queryString) {
		queryParameter.setWhereCondition(true);
		String pattern = "(.*)";
		List<Criteria> listCriteria = new ArrayList<>();
		String[] whereArrayConditions = null;
		List<String> logicalOperator = new ArrayList<>();
		String[] patternrelation;
		Pattern wherePattern = Pattern.compile(pattern);
		Matcher whereMatcher = wherePattern.matcher(queryString);

		if (whereMatcher.find()) {
			whereArrayConditions = queryString.split(" ");

			for (String condition : whereArrayConditions) {
				
				 
				if ((condition.trim().equalsIgnoreCase("and")) ||(condition.trim().equalsIgnoreCase("or")))
				{
					logicalOperator.add(condition.trim());

				} else {
					Criteria criteria = new Criteria();
					patternrelation = condition.split("([<|>|!|=])+");		
					criteria.setColumnName(patternrelation[0]);
					criteria.setValue(patternrelation[1]);
					
					int startIndex = condition.indexOf(patternrelation[0]) + patternrelation[0].length();
					int endIndex = condition.indexOf(patternrelation[1]);
					String operator = condition.substring(startIndex, endIndex).trim();	
					criteria.setOperator(operator);
					listCriteria.add(criteria);
				}
			}
			queryParameter.setRestrictions(listCriteria);
		}
		
		queryParameter.setLogicalOperators(logicalOperator);
		
		return queryParameter;

	}

	private String groupbyClause(String queryString) {
		Pattern pattern = Pattern.compile("(.*\\S)");
		Matcher matcher = pattern.matcher(queryString);

		String groupByString = null;

		if (matcher.find()) {
			groupByString = matcher.group(1);

		}
		return groupByString;

	}

	private String havingClause(String queryString) {
		// TODO Auto-generated method stub
		Pattern pattern = Pattern.compile("(.*\\S)");
		Matcher matcher = pattern.matcher(queryString);

		String havingString = null;

		if (matcher.find()) {
			havingString = matcher.group(1);

		}	
		return havingString;
	}

	private String orderbyClause(String queryString) {

		Pattern pattern1 = Pattern.compile("(.*\\S)");
		Matcher matcher = pattern1.matcher(queryString);

		String orderbyString = null;

		if (matcher.find()) {
			orderbyString = matcher.group(1);

		}
		return orderbyString;

	}
	
}
