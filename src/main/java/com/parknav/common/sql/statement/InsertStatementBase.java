package com.parknav.common.sql.statement;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Optional;
import java.util.stream.Collectors;

import com.parknav.common.sql.DMLColumn;
import com.parknav.common.sql.orderby.OrderByTerm;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.TextStringBuilder;

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

		Optional.ofNullable(buildOnConflictExpression())
			.filter(StringUtils::isNotBlank)
			.ifPresent(builder::append);

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

	/**
	 * Builds whole ON CONFLICT expression. If non-null, string will be terminated with newline.
	 *
	 * @return built ON CONFLICT expression
	 */
	protected String buildOnConflictExpression() {
		return null;
	}

}
