package com.parknav.common.sql.statement;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.parknav.common.sql.orderby.OrderByTerm;
import com.parknav.common.sql.orderby.SimpleOrderByTerm;
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

		Optional.ofNullable(buildOrderByExpression())
			.filter(StringUtils::isNotBlank)
			.ifPresent(builder::append);

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
		if (columns == null)
			throw new NullPointerException();
		this.distinctOnColumns.clear();
		return addDistinctOnColumns(columns);
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
		return joinExpressions;
	}
	
	public SelectStatement setJoinExpressions(Collection<String> expressions) {
		if (expressions == null)
			throw new NullPointerException();
		this.joinExpressions.clear();
		return addJoinExpressions(expressions);
	}
	
	public SelectStatement addJoinExpressions(String... expressions) {
		return addJoinExpressions(Arrays.asList(expressions));
	}
	
	public SelectStatement addJoinExpressions(Collection<String> expressions) {
		this.joinExpressions.addAll(expressions);
		return self();
	}

	// ORDER BY

	public List<OrderByTerm> getOrderByTerms() {
		return orderByTerms;
	}

	public SelectStatement setOrderByTerms(Collection<OrderByTerm> orderByTerms) {
		if (orderByTerms == null)
			throw new NullPointerException();
		this.orderByTerms.clear();
		return addOrderByTerms(orderByTerms);
	}

	public SelectStatement addOrderByTerms(OrderByTerm... orderByTerms) {
		addOrderByTerms(Arrays.asList(orderByTerms));
		return self();
	}

	public SelectStatement addOrderByTerms(Collection<OrderByTerm> orderByTerms) {
		for (OrderByTerm term : orderByTerms) {
			term.setStatement(this);
			this.orderByTerms.add(term);
		}
		return self();
	}

	// ORDER BY (simplified, plain strings)

	public SelectStatement setOrderByExpressions(Collection<String> orderByExpressions) {
		if (orderByExpressions == null)
			throw new NullPointerException();
		this.orderByTerms.clear();
		return addOrderByExpressions(orderByExpressions);
	}

	public SelectStatement addOrderByExpressions(String... orderByExpressions) {
		addOrderByExpressions(Arrays.asList(orderByExpressions));
		return self();
	}

	public SelectStatement addOrderByExpressions(Collection<String> orderByExpressions) {
		return addOrderByTerms(
			orderByExpressions
				.stream()
				.map(expression -> new SimpleOrderByTerm().add(builder -> builder.formatln(expression)))
				.collect(Collectors.toList())
		);
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

	@Override
	public void setValues(PreparedStatement statement) throws SQLException {

		int index = 0;
		
		index = setWhereValues(statement, index);

		//noinspection UnusedAssignment
		index = setOrderByValues(statement, index);

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

	/**
	 * Builds whole ORDER BY expression by joining clauses from all {@link OrderByTerm}s. If non-null, string will be terminated with newline.
	 *
	 * @return built ORDER BY expression
	 */
	protected String buildOrderByExpression() {

		if (orderByTerms.isEmpty())
			return null;

		String expression = getOrderByTerms()
			.stream()
			.map(OrderByTerm::build)
			.map(StringUtils::stripToNull)	// NOTE strips leading tab and trailing newline, too!
			.filter(Objects::nonNull)
			.map(this::prefixTab)																	// prefix first line
			.map(str -> str.replaceAll("\n", "\n".concat(StringUtils.repeat('\t', getLevel()))))	// prefix each new line
			.collect(Collectors.joining(",\n"));

		if (StringUtils.isBlank(expression))
			return null;

		return new TextStringBuilder()
			.appendln("ORDER BY")
			.appendln(expression)	// StringUtils.stripToNull stripped trailing newline, so re-apply it
			.toString();

	}

	protected int setOrderByValues(PreparedStatement statement, int index) throws SQLException {
		for (OrderByTerm term : getOrderByTerms())
			index = term.setValues(statement, index);
		return index;
	}

	@Override
	protected SelectStatement self() { return this; }

	private final List<String> distinctOnColumns = new ArrayList<>();
	private final List<String> joinExpressions = new ArrayList<>();
	private final List<OrderByTerm> orderByTerms = new ArrayList<>();

	private String limitExpression;
	private String offsetExpression;

}
