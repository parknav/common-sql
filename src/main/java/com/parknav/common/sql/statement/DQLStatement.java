package com.parknav.common.sql.statement;

public abstract class DQLStatement<S extends DQLStatement<S>> extends SQLWhereStatement<S> {

	public DQLStatement(String tableName) {
		super(tableName);
	}

}
