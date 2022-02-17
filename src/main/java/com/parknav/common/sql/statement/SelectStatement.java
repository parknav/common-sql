package com.parknav.common.sql.statement;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.parknav.common.sql.where.WhereTerm;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.TextStringBuilder;

public class SelectStatement extends DQLStatement<SelectStatement> {

	public SelectStatement(String tableName) {
		super(tableName);
	}

	@Override
	public String buildExpression() {
		
		TextStringBuilder builder = new TextStringBuilder();

		builder.appendln("SELECT");
		
		Optional.of(String.join(", ", getDistinctOnColumns()))
			.filter(StringUtils::isNotBlank)
			.map(columns -> "DISTINCT ON (" + columns + ")")
			.map(this::prefixTab)
			.ifPresent(builder::appendln);
		
		Optional.of(String.join(", ", getDQLColumns()))
			.map(this::prefixTab)
			.ifPresent(builder::appendln);
		
		builder.append("FROM ").appendln(getTableName());
		
		Optional.ofNullable(buildJoinExpression())
			.filter(StringUtils::isNotBlank)
			.ifPresent(builder::append);
		
		Optional.ofNullable(buildWhereExpression())
			.filter(StringUtils::isNotBlank)
			.ifPresent(builder::append);

		Optional.of(String.join(", ", getOrderByExpressions()))
			.filter(StringUtils::isNotBlank)
			.map(expressions -> "ORDER BY " + expressions)
			.ifPresent(builder::appendln);

		Optional.ofNullable(limitExpression)
			.map(expression -> "LIMIT " + expression)
			.ifPresent(builder::appendln);

		Optional.ofNullable(offsetExpression)
			.map(expression -> "OFFSET " + expression)
			.ifPresent(builder::appendln);
		
		return builder.toString();

	}

	// DISTINCT ON
	
	public List<String> getDistinctOnColumns() {
		return distinctOnColumns;
	}
	
	public SelectStatement setDistinctOnColumns(List<String> columns) {
		this.distinctOnColumns = columns;
		return self();
	}
	
	public SelectStatement addDistinctOnColumns(String... columns) {
		return addDistinctOnColumns(Arrays.asList(columns));
	}
	
	public SelectStatement addDistinctOnColumns(Collection<String> columns) {
		this.distinctOnColumns.addAll(columns);
		return self();
	}

	/**
	 * Adds columns prefixed with this statement's table name.
	 *
	 * @param columns columns to add
	 *
	 * @return self
	 */
	public SelectStatement addTableDistinctOnColumns(String... columns) {
		return addTableDistinctOnColumns(Arrays.asList(columns));
	}

	/**
	 * Adds columns prefixed with this statement's table name.
	 *
	 * @param columns columns to add
	 *
	 * @return self
	 */
	public SelectStatement addTableDistinctOnColumns(Collection<String> columns) {
		return addTableDistinctOnColumns(columns, null);
	}

	/**
	 * Adds columns prefixed with this statement's table name and decorated with provided decorator.
	 *
	 * @param columns columns to add
	 * @param decorator decorator for generated column tokens
	 *
	 * @return self
	 */
	public SelectStatement addTableDistinctOnColumns(Collection<String> columns, Function<String, String> decorator) {
		addDistinctOnColumns(columns.stream().map(this::toTableColumn).map(tableColumn -> decorator != null ? decorator.apply(tableColumn) : tableColumn).collect(Collectors.toList()));
		return self();
	}

	// JOIN
	
	public List<String> getJoinExpressions() {
		return orderByExpressions;
	}
	
	public SelectStatement setJoinExpressions(List<String> expressions) {
		this.orderByExpressions = expressions;
		return self();
	}
	
	public SelectStatement addJoinExpressions(String... expressions) {
		return addJoinExpressions(Arrays.asList(expressions));
	}
	
	public SelectStatement addJoinExpressions(Collection<String> expressions) {
		this.joinExpressions.addAll(expressions);
		return self();
	}

	// ORDER BY
	
	public List<String> getOrderByExpressions() {
		return orderByExpressions;
	}
	
	public SelectStatement setOrderByExpressions(List<String> expressions) {
		this.orderByExpressions = expressions;
		return self();
	}
	
	public SelectStatement addOrderByExpressions(String... expressions) {
		return addOrderByExpressions(Arrays.asList(expressions));
	}
	
	public SelectStatement addOrderByExpressions(Collection<String> expressions) {
		this.orderByExpressions.addAll(expressions);
		return self();
	}

	// LIMIT
	
	public SelectStatement setLimit(Integer limit) {
		this.limitExpression = Optional.ofNullable(limit).map(Object::toString).orElse(null);
		return self();
	}

	// OFFSET

	public SelectStatement setOffset(Integer offset) {
		this.offsetExpression = Optional.ofNullable(offset).map(Object::toString).orElse(null);
		return self();
	}

	protected String buildJoinExpression() {

		TextStringBuilder builder = new TextStringBuilder();
		
		// own JOINs
		for (String join : joinExpressions)
			builder.appendSeparator('\n').appendln(join);
			
		// JOINs from WHERE terms
		for (WhereTerm term : getWhereTerms())
			for (String join : term.getJoinExpressions())
				builder.appendSeparator('\n').appendln(join);
		
		return builder.toString();
		
	}

	@Override
	public void setValues(PreparedStatement statement) throws SQLException {

		int index = 0;
		
		index = setWhereValues(statement, index);
		
	}

	@Override
	protected SelectStatement self() { return this; }

	private List<String> distinctOnColumns = new ArrayList<>();
	private List<String> joinExpressions = new ArrayList<>();
	private List<String> orderByExpressions = new ArrayList<>();
	private String limitExpression;
	private String offsetExpression;
	
}
