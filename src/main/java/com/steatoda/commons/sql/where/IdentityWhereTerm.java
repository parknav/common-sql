package com.steatoda.commons.sql.where;

public abstract class IdentityWhereTerm<I> extends WhereTerm {

	public IdentityWhereTerm(String column, I id) {
		this.column = column;
		this.id = id;
	}

	public String getColumn() { return column; }
	public I getId() { return id; }

	@Override
	public String build() {
		
		return getQueryBuilder().formatln("AND %s = ?", column).toString();
		
	}

	private final String column;
	private final I id;
	
}
