package com.steatoda.commons.sql.statement;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.TextStringBuilder;

public class DeleteStatement extends DMLStatement<DeleteStatement> {

	public DeleteStatement(String tableName) {
		super(tableName);
	}

	public boolean getRequireWhereExpression() { return requireWhereExpression; }
	public DeleteStatement setRequireWhereExpression(boolean requireWhereExpression) { this.requireWhereExpression = requireWhereExpression; return self(); }

	@Override
	public String buildExpression() {

		String whereExpression = buildWhereExpression();
		if (requireWhereExpression && StringUtils.isBlank(whereExpression))
			throw new IllegalStateException("This DeleteStatement is configured to require WHERE expression, but it's missing");
		
		TextStringBuilder builder = new TextStringBuilder(1000);

		builder
			.append("DELETE FROM ").appendln(getTableName())
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
		
		index = setWhereValues(statement, index);
		
	}

	@Override
	protected DeleteStatement self() { return this; }

	private boolean requireWhereExpression = true;
	
}
