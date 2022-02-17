package com.parknav.common.sql.statement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.parknav.common.sql.DMLColumn;

public abstract class DMLStatement<S extends DMLStatement<S>> extends SQLWhereStatement<S> {

	public DMLStatement(String tableName) {
		super(tableName);
	}

	public List<DMLColumn> getDMLColumns() {
		return dmlColumns;
	}

	public S addDMLColumn(String columnName, String columnValue, DMLColumn.Initializer initializer) {
		return addDMLColumn(columnName, columnValue, columnValue, initializer);
	}
	
	public S addDMLColumn(String columnName, String columnInsertValue, String columnUpdateValue, DMLColumn.Initializer initializer) {
		return addDMLColumns(new DMLColumn(columnName, columnInsertValue, columnUpdateValue, initializer));
	}
	
	public S addDMLColumns(DMLColumn... columns) {
		return addDMLColumns(Arrays.asList(columns));
	}
	
	public S addDMLColumns(List<DMLColumn> columns) {
		this.dmlColumns.addAll(columns);
		return self();
	}

	private final List<DMLColumn> dmlColumns = new ArrayList<>();

}
