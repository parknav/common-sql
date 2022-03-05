package com.parknav.common.sql.orderby;

import com.parknav.common.sql.HasTableName;
import com.parknav.common.sql.TableTerm;
import com.parknav.common.sql.statement.SQLTableStatement;

public abstract class OrderByTerm extends TableTerm implements HasTableName {

	@Override
	public SQLTableStatement<?> getStatement() { return statement; }

	public OrderByTerm setStatement(SQLTableStatement<?> statement) { this.statement = statement; return this; }
	
	private SQLTableStatement<?> statement = null;

}
