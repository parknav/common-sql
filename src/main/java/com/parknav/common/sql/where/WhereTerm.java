package com.parknav.common.sql.where;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.parknav.common.sql.HasTableName;
import org.apache.commons.text.TextStringBuilder;

import com.parknav.common.sql.statement.SQLWhereStatement;

public abstract class WhereTerm implements HasTableName {

	private static class Join {
		public Join(String tableName, String tableNameAlias, String joinType, Function<String, String> joinConditionFunc) {
			this.tableName = tableName;
			this.tableNameAlias = tableNameAlias;
			this.joinType = joinType;
			this.joinConditionFunc = joinConditionFunc;
		}
		@Override
		public String toString() {
			return new TextStringBuilder()
				.setNullText(null)
				.append(joinType).appendSeparator(' ')
				.append("JOIN").appendSeparator(' ')
				.append(tableName).appendSeparator(' ')
				.append(tableNameAlias).appendSeparator(' ')
				.append("ON").appendSeparator(' ')
				.append(joinConditionFunc.apply(tableNameAlias))
				.toString();
		}
		private final String tableName;
		private final String tableNameAlias;
		private final String joinType;
		private final Function<String, String> joinConditionFunc;
	}
	
	public SQLWhereStatement<?> getStatement() { return statement; }
	public WhereTerm setStatement(SQLWhereStatement<?> statement) { this.statement = statement; return this; }
	
	@Override
	public String getTableName() { return tableName; }

	/**
	 * Sets table name which this WhereTerm should reference (it has to be manually added to SELECT)
	 *
	 * @param tableName table name this WHERE term references
	 *
	 * @return self
	 */
	public WhereTerm setTableName(String tableName) { this.tableName = tableName; return this; }

	/**
	 * <p>Sets table name which this WhereTerm should reference and adds JOIN reference for that table.</p>
	 * <p>NOTE: table name will be aliased to be unique</p>
	 *
	 * @param tableName table this WHERE term references (aliased)
	 * @param joinConditionFunc mapper that generates JOIN expression
	 *
	 * @return self
	 */
	public WhereTerm setTableName(String tableName, Function<String, String> joinConditionFunc) {
		return setTableName(tableName, null, joinConditionFunc);
	}
	
	public WhereTerm setTableName(String tableName, String joinType, Function<String, String> joinConditionFunc) {
		String tableNameAlias = addJoin(tableName, joinType, joinConditionFunc);
		setTableName(tableNameAlias);
		return this;
	}

	/**
	 * <p>Returns table which this {@code WhereTerm} references, chosen in following order (first non-{@code null} applies):</p>
	 * <ul>
	 * 	<li>{@code WhereTerm}'s own table name</li>
	 * 	<li>table name from {@code WhereTerm}'s statement</li>
	 * </ul>
	 *
	 * @return table which this {@code WhereTerm} references
	 */
	public String getEffectiveTableName() {
		if (tableName != null)
			return tableName;
		return statement.getTableName();
	}

	public List<String> getJoinExpressions() {
		return joins.stream()
			.map(Join::toString)
			.collect(Collectors.toList())
		;
	}

	/**
	 * <p>Creates new JOIN reference.</p>
	 * <p>NOTE: table name will be aliased to be unique</p>
	 *
	 * @param tableName table name (aliases)
	 * @param joinConditionFunc mapper that generates JOIN expression
	 *
	 * @return aliased table name
	 */
	protected String addJoin(String tableName, Function<String, String> joinConditionFunc) {
		return addJoin(tableName, null, joinConditionFunc);
	}
	
	protected String addJoin(String tableName, String joinType, Function<String, String> joinConditionFunc) {
		String tableNameAlias = String.format("%s_%s", tableName, UUID.randomUUID()).replaceAll("-", "_");
		// don't calculate JOIN expressions now, since joinFunc will most certainly depend on statement being set (for it's table name)
		// instead, store parameters and generate expression on demand
		joins.add(new Join(tableName, tableNameAlias, joinType, joinConditionFunc));
		return tableNameAlias;
	}

	/**
	 * Builds this term's WHERE clauses. If non-{@code null}, string have to be terminated with newline.
	 *
	 * @return constructed WHERE clauses
	 */
	public abstract String build();
	
	public abstract int setValues(PreparedStatement statement, int offset) throws SQLException;

	/**
	 * <p>Prefixes column name with table name which is chooses in following order (first non-{@code null} applies):</p>
	 * <ul>
	 *  <li>{@code WhereTerm}'s own table name</li>
	 *	<li>table name from {@code WhereTerm}'s statement</li>
	 * </ul>
	 *
	 * @param column column name
	 *
	 * @return column name prefixed with table name
	 */
	@Override
	public String toTableColumn(String column) {
		return Optional.ofNullable(getEffectiveTableName())
			.map(tableName -> tableName + "." + column)
			.orElse(column)
		;
	}

	private SQLWhereStatement<?> statement = null;
	private String tableName = null;
	private List<Join> joins = new ArrayList<>();

}
