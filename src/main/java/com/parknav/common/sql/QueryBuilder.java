package com.parknav.common.sql;

import java.util.stream.Stream;

import org.apache.commons.text.TextStringBuilder;

public class QueryBuilder {

	public QueryBuilder(HasTableName context) {
		this.context = context;
		this.builder = new TextStringBuilder();
	}

	public QueryBuilder appendNewLine() {
		builder.appendNewLine();
		return this;
	}
	
	public QueryBuilder append(final String str) {
		builder.append(str);
		return this;
	}
	
	public QueryBuilder appendln(final String str) {
		return append(str).appendNewLine();
	}

	/**
	 * Appends {@code fmt} populated with column names with prepended proper table name.
	 *
	 * @param fmt format
	 * @param columns columns to append
	 *
	 * @return this
	 */
	public QueryBuilder format(final String fmt, String... columns) {
		builder.append(String.format(fmt, Stream.of(columns).map(context::toTableColumn).toArray(Object[]::new)));
		return this;
	}

	/**
	 * Appends {@code fmt} populated with column names with prepended proper table name and newline at the end.
	 *
	 * @param fmt format
	 * @param columns columns to append
	 *
	 * @return this
	 */
	public QueryBuilder formatln(final String fmt, String... columns) {
		return format(fmt, columns).appendNewLine();
	}

	@Override
	public String toString() {
		return builder.toString();
	}
	
	private final HasTableName context;
	private final TextStringBuilder builder;
	
}
