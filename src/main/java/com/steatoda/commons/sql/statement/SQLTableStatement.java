package com.steatoda.commons.sql.statement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.steatoda.commons.sql.HasTableName;

public abstract class SQLTableStatement<S extends SQLTableStatement<S>> extends SQLStatement<S> implements HasTableName {

	public SQLTableStatement(String tableName) {
		this.tableName = tableName;
	}

	@Override
	public String getTableName() {
		return tableName;
	}

	/** Prefixes column name with table name */
	@Override
	public String toTableColumn(String column) {
		return tableName != null ? tableName + "." + column : column;
	}

	public List<String> getDQLColumns() {
		return dqlColumns;
	}
	
	public S setDQLColumns(List<String> columns) {
		this.dqlColumns = columns;
		return self();
	}
	
	public S addDQLColumns(String... columns) {
		return addDQLColumns(Arrays.asList(columns));
	}
	
	public S addDQLColumns(Collection<String> columns) {
		this.dqlColumns.addAll(columns);
		return self();
	}

	/**
	 * Adds columns prefixed with this statement's table name.
	 *
	 * @param columns columns to add
	 *
	 * @return self
	 */
	public S addTableDQLColumns(String... columns) {
		return addTableDQLColumns(Arrays.asList(columns));
	}

	/**
	 * Adds columns prefixed with this statement's table name.
	 *
	 * @param columns columns to add
	 *
	 * @return self
	 */
	public S addTableDQLColumns(Collection<String> columns) {
		return addTableDQLColumns(columns, null);
	}

	/**
	 * Adds columns prefixed with this statement's table name and decorated with provided decorator.
	 *
	 * @param columns columns to add
	 * @param decorator decorator for generated column tokens
	 *
	 * @return self
	 */
	public S addTableDQLColumns(Collection<String> columns, Function<String, String> decorator) {
		addDQLColumns(columns.stream().map(this::toTableColumn).map(tableColumn -> decorator != null ? decorator.apply(tableColumn) : tableColumn).collect(Collectors.toList()));
		return self();
	}

	private final String tableName;

	private List<String> dqlColumns = new ArrayList<>();

}
