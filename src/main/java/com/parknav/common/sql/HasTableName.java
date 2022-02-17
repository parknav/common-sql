package com.parknav.common.sql;

public interface HasTableName {

	String getTableName();

	String toTableColumn(String column);

	default QueryBuilder getQueryBuilder() {
		return new QueryBuilder(this);
	}
	
}
