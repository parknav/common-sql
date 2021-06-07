package com.steatoda.commons.sql.statement;

public class InsertStatement extends InsertStatementBase<InsertStatement> {
	
	public InsertStatement(String tableName) {
		super(tableName);
	}

	@Override
	protected InsertStatement self() { return this; }

}
