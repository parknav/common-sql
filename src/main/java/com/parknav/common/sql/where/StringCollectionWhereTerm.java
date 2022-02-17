package com.parknav.common.sql.where;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class StringCollectionWhereTerm extends CollectionWhereTerm {

	public StringCollectionWhereTerm(String column, String[] ids) {
		super(column);
		this.ids = ids;
	}

	@Override
	public int setValues(PreparedStatement statement, int index) throws SQLException {

		statement.setArray(++index, statement.getConnection().createArrayOf("text", ids));
		
		return index;

	}

	private final String[] ids;
	
}
