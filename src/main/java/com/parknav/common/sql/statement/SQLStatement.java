package com.parknav.common.sql.statement;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SQLStatement<S extends SQLStatement<S>> {

	public int getLevel() { return level; }

	public void setLevel(int level) { this.level = level; }

	public abstract String buildExpression();

	public PreparedStatement build(Connection conn) throws SQLException {
		String sql = buildExpression();
		Log.trace("Built SQL statement:\n{}", sql);
		PreparedStatement statement = prepareStatement(sql, conn);
		setValues(statement);
		return statement;
	}

	public PreparedStatement prepareStatement(String sql, Connection conn) throws SQLException {
		return conn.prepareStatement(sql);
	}
	
	public void setValues(PreparedStatement statement) throws SQLException { /* no-op */ }

	public String prefixTab(String str) {
		return prefixTab(str, getLevel());
	}
	
	public static String prefixTab(String str, int level) {
		return StringUtils.repeat('\t', level).concat(str);
	}
	
	protected abstract S self();

	private static final Logger Log = LoggerFactory.getLogger(SQLStatement.class);

	private int level = 1;

}
