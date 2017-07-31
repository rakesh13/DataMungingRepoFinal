package com.datamunging.query.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Query {

	private String groupBy;
	private String orderBy;
	private String filePath;
	private boolean allColumns;
	private List<Criteria> restrictions;
	private List<AggregateFunction> aggregateFunctions;
	private boolean whereCondition;
	List<String> logicalOperators;;
	private boolean isAggregate;
	private List<String> fields;;
	private String queryType;
	public String getGroupBy() {
		return groupBy;
	}
	public void setGroupBy(String groupBy) {
		this.groupBy = groupBy;
	}
	public String getOrderBy() {
		return orderBy;
	}
	public void setOrderBy(String orderBy) {
		this.orderBy = orderBy;
	}
	public String getFilePath() {
		return filePath;
	}
	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}
	public boolean isAllColumns() {
		return allColumns;
	}
	public void setAllColumns(boolean allColumns) {
		this.allColumns = allColumns;
	}
	public List<Criteria> getRestrictions() {
		return restrictions;
	}
	public void setRestrictions(List<Criteria> restrictions) {
		this.restrictions = restrictions;
	}
	public List<AggregateFunction> getAggregateFunctions() {
		return aggregateFunctions;
	}
	public void setAggregateFunctions(List<AggregateFunction> aggregateFunctions) {
		this.aggregateFunctions = aggregateFunctions;
	}
	public boolean isWhereCondition() {
		return whereCondition;
	}
	public void setWhereCondition(boolean whereCondition) {
		this.whereCondition = whereCondition;
	}
	public List<String> getLogicalOperators() {
		return logicalOperators;
	}
	public void setLogicalOperators(List<String> logicalOperators) {
		this.logicalOperators = logicalOperators;
	}
	public boolean isAggregate() {
		return isAggregate;
	}
	public void setAggregate(boolean isAggregate) {
		this.isAggregate = isAggregate;
	}
	public List<String> getFields() {
		return fields;
	}
	public void setFields(List<String> fields) {
		this.fields = fields;
	}
	public String getQueryType() {
		return queryType;
	}
	public void setQueryType(String queryType) {
		this.queryType = queryType;
	}
	
	
	
	
}
