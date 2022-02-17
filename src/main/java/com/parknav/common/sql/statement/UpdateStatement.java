package com.parknav.common.sql.statement;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.stream.Collectors;

import com.parknav.common.sql.DMLColumn;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.TextStringBuilder;

public class UpdateStatement extends DMLStatement<UpdateStatement> {
	
	public UpdateStatement(String tableName) {
		super(tableName);
	}

	public boolean getRequireWhereExpression() { return requireWhereExpression; }
	public UpdateStatement setRequireWhereExpression(boolean requireWhereExpression) { this.requireWhereExpression = requireWhereExpression; return self(); }

	@Override
	public String buildExpression() {
		
		if (getDMLColumns().isEmpty())
			throw new IllegalStateException("No columns defined to update");
		
		String whereExpression = buildWhereExpression();
		if (requireWhereExpression && StringUtils.isBlank(whereExpression))
			throw new IllegalStateException("This UpdateStatement is configured to require WHERE expression, but it's missing");

		TextStringBuilder builder = new TextStringBuilder();

		builder
			.append("UPDATE ").appendln(getTableName())
			.appendln("SET")
			.appendln(getDMLColumns().stream().map(column -> column.getName() + "=" + column.getUpdateValue()).map(this::prefixTab).collect(Collectors.joining(",\n")))
			.appendln(whereExpression)
		;

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

		index = setWhereValues(statement, index);
		
	}

	@Override
	protected UpdateStatement self() { return this; }

	private boolean requireWhereExpression = true;
	
}
