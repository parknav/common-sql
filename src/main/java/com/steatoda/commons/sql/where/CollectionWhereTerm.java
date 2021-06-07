package com.steatoda.commons.sql.where;

public abstract class CollectionWhereTerm extends WhereTerm {

	public CollectionWhereTerm(String column) {
		this.column = column;
	}

	@Override
	public String build() {

		return getQueryBuilder().formatln("AND %s = ANY(?)", column).toString();

	}

	private final String column;
	
}
