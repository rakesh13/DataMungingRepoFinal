package com.datamunging;

import java.util.List;

import com.datamunging.query.parser.Query;

public interface DataReader {
	
	List<DataRow> executeQuery(Query queryParameter);

}
