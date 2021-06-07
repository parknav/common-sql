package com.steatoda.commons.sql;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DMLColumn {

	@FunctionalInterface
	public interface Initializer {
		int setValue(PreparedStatement statement, int index) throws SQLException;
	}

	public DMLColumn(String name, String value, Initializer initializer) {
		this(name, value, value, initializer);
	}
	
	public DMLColumn(String name, String insertValue, String updateValue, Initializer initializer) {
		this.name = name;
		this.insertValue = insertValue;
		this.updateValue = updateValue;
		this.initializer = initializer;
	}
	
	public String getName() { return name; }
	public String getInsertValue() { return insertValue; }
	public String getUpdateValue() { return updateValue; }
	public Initializer getInitializer() { return initializer; }
	
	private final String name;
	private final String insertValue;
	private final String updateValue;
	private final Initializer initializer;
	
}
