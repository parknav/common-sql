package com.steatoda.commons.sql.where;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class StringIdentityWhereTerm extends IdentityWhereTerm<String> {

	public StringIdentityWhereTerm(String column, String id) {
		super(column, id);
	}

	@Override
	public int setValues(PreparedStatement statement, int index) throws SQLException {

		statement.setString(++index, getId());
		
		return index;
		
	}

}
