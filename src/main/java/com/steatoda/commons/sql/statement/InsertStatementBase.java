package com.steatoda.commons.sql.statement;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.stream.Collectors;

import org.apache.commons.text.TextStringBuilder;

import com.steatoda.commons.sql.DMLColumn;

public abstract class InsertStatementBase<S extends InsertStatementBase<S>> extends DMLStatement<S> {
	
	public InsertStatementBase(String tableName) {
		super(tableName);
	}

	@Override
	public String buildExpression() {
		
		if (getDMLColumns().isEmpty())
			throw new IllegalStateException("No DML columns defined to insert");
		
		TextStringBuilder builder = new TextStringBuilder();

		builder
			.append("INSERT INTO ").append(getTableName()).appendln(" (")
				.appendln(getDMLColumns().stream().map(DMLColumn::getName).map(this::prefixTab).collect(Collectors.joining(",\n")))
			.appendln(") VALUES (")
				.appendln(getDMLColumns().stream().map(DMLColumn::getInsertValue).map(this::prefixTab).collect(Collectors.joining(",\n")))
			.appendln(")")
		;

		builder.appendln(buildOnConflictExpression());
		
		if (!getDQLColumns().isEmpty())
			builder
				.appendln("RETURNING")
				.appendln(getDQLColumns().stream().map(this::prefixTab).collect(Collectors.joining(",\n")));

		return builder.toString();
		
	}

	@Override
	public void setValues(PreparedStatement statement) throws SQLException {

		int index = 0;
		
		for (DMLColumn column : getDMLColumns())
			if (column.getInitializer() != null)
				index = column.getInitializer().setValue(statement, index);

	}

	protected String buildOnConflictExpression() {
		return null;
	}

}
