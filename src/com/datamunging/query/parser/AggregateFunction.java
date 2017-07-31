package com.datamunging.query.parser;

public class AggregateFunction {

	private String aggregateFunctionColumn;
	private int result;
	private String functionName;
	private int aggregateFieldIndex;
	
	public String getAggregateFunctionColumn() {
		return aggregateFunctionColumn;
	}
	public void setAggregateFunctionColumn(String aggregateFunctionColumn) {
		this.aggregateFunctionColumn = aggregateFunctionColumn;
	}
	public int getResult() {
		return result;
	}
	public void setResult(int result) {
		this.result = result;
	}
	public String getFunctionName() {
		return functionName;
	}
	public void setFunctionName(String functionName) {
		this.functionName = functionName;
	}
	public int getAggregateFieldIndex() {
		return aggregateFieldIndex;
	}
	public void setAggregateFieldIndex(int aggregateFieldIndex) {
		this.aggregateFieldIndex = aggregateFieldIndex;
	}
	
	
}
