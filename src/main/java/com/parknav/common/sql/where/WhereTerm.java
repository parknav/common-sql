package com.parknav.common.sql.where;

import com.parknav.common.sql.HasTableName;

import com.parknav.common.sql.TableTerm;
import com.parknav.common.sql.statement.SQLWhereStatement;

public abstract class WhereTerm extends TableTerm implements HasTableName {

	@Override
	public SQLWhereStatement<?> getStatement() { return statement; }

	public WhereTerm setStatement(SQLWhereStatement<?> statement) { this.statement = statement; return this; }
	
	private SQLWhereStatement<?> statement = null;

}
