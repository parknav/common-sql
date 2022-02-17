package com.parknav.common.sql.where;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

public class UUIDCollectionWhereTerm extends CollectionWhereTerm {

	public UUIDCollectionWhereTerm(String column, UUID[] ids) {
		super(column);
		this.ids = ids;
	}

	@Override
	public int setValues(PreparedStatement statement, int index) throws SQLException {

		statement.setArray(++index, statement.getConnection().createArrayOf("uuid", ids));
		
		return index;

	}

	private final UUID[] ids;
	
}
