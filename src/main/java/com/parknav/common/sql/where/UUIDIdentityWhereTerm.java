package com.parknav.common.sql.where;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

public class UUIDIdentityWhereTerm extends IdentityWhereTerm<UUID> {

	public UUIDIdentityWhereTerm(String column, UUID id) {
		super(column, id);
	}

	@Override
	public int setValues(PreparedStatement statement, int index) throws SQLException {

		statement.setObject(++index, getId());
		
		return index;
		
	}

}
