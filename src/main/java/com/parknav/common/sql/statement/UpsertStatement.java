package com.parknav.common.sql.statement;

import com.parknav.common.sql.DMLColumn;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.TextStringBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class UpsertStatement extends InsertStatementBase<UpsertStatement> {

	public UpsertStatement(String tableName) {
		super(tableName);
	}

	public String getConflictTarget() {
		return conflictTarget;
	}

	public UpsertStatement setConflictTarget(String conflictTarget) {
		this.conflictTarget = conflictTarget;
		return self();
	}

	public List<String> getUpsertSetColumns() {
		return upsertSetColumns;
	}

	public UpsertStatement setUpsertSetColumns(List<String> columns) {
		this.upsertSetColumns = columns;
		return self();
	}

	public UpsertStatement addUpsertSetColumns(String... columns) {
		return addUpsertSetColumns(Arrays.asList(columns));
	}

	public UpsertStatement addUpsertSetColumns(Collection<String> columns) {
		this.upsertSetColumns.addAll(columns);
		return self();
	}

	public List<String> getUpsertWhereColumns() {
		return upsertWhereColumns;
	}

	public UpsertStatement setUpsertWhereColumns(List<String> columns) {
		this.upsertWhereColumns = columns;
		return self();
	}

	public UpsertStatement addUpsertWhereColumns(String... columns) {
		return addUpsertWhereColumns(Arrays.asList(columns));
	}

	public UpsertStatement addUpsertWhereColumns(Collection<String> columns) {
		this.upsertWhereColumns.addAll(columns);
		return self();
	}

	@Override
	public String buildExpression() {

		if (getDQLColumns().isEmpty() || !upsertSetColumns.isEmpty())
			return super.buildExpression();	// it's safe to use regular syntax

		/*
		 * Now shit is about to hit the fan...
		 *
		 * We have DQL columns defined (this statement *has* to return something),
		 * but ON CONFLICT will DO NOTHING (because upsertSetColumns.isEmpty()) and subsequent RETURNING will not return anything.
		 *
		 * Solution is to use this workaround: https://stackoverflow.com/a/40325406/4553548
		 *
		 * Note (from SO answer): However, there is still a tiny corner case for a race condition.
		 * Concurrent transactions may have added a conflicting row, which is not yet visible
		 * in the same statement. Then INSERT and SELECT come up empty.
		 */

		if (getDMLColumns().isEmpty())
			throw new IllegalStateException("No DML columns defined to insert");

		if (StringUtils.isBlank(conflictTarget))
			throw new IllegalStateException("UPSERT must have conflict target defined");

		if (upsertWhereColumns.isEmpty())
			throw new IllegalStateException("UPSERT must have at least one WHERE column defined");

		// check that DML columns contains every column from upsertWhereColumns
		for (String whereColumn : upsertWhereColumns) {
			boolean found = false;
			for (DMLColumn dmlColumn : getDMLColumns()) {
				if (dmlColumn.getName().equals(whereColumn)) {
					found = true;
					break;
				}
			}
			if (!found)
				throw new IllegalStateException("UPSERT WHERE column '" + whereColumn + "' doesn't have matching DMLColumn");
		}

		TextStringBuilder builder = new TextStringBuilder();

		builder
			.appendln("WITH _data AS (")
			.append('\t').appendln("SELECT")
			.appendln(getDMLColumns().stream().map(dmlColumn -> dmlColumn.getInsertValue().concat(" AS ").concat(dmlColumn.getName())).map(this::prefixTab).map(this::prefixTab).collect(Collectors.joining(",\n")))
			.appendln("), _inserted_row AS (")
			.append('\t').append("INSERT INTO ").append(getTableName()).appendln(" (")
			.appendln(getDMLColumns().stream().map(DMLColumn::getName).map(this::prefixTab).map(this::prefixTab).collect(Collectors.joining(",\n")))
			.append('\t').appendln(") SELECT")
			.appendln(getDMLColumns().stream().map(DMLColumn::getName).map(this::prefixTab).map(this::prefixTab).collect(Collectors.joining(",\n")))
			.append('\t').appendln("FROM _data")
			.append('\t').append("ON CONFLICT (").append(conflictTarget).appendln(") DO UPDATE SET")
			.append('\t').append('\t').append(getDQLColumns().get(0)).appendln(" = NULL")	// pick first DQL column for dummy update (which WILL NOT be performed)
			.append('\t').appendln("WHERE FALSE")	// prevent this UPDATE from actually updating anything, but let it lock the row
			.append('\t').appendln("RETURNING")
			.appendln(getDQLColumns().stream().map(this::prefixTab).map(this::prefixTab).collect(Collectors.joining(",\n")))
			.appendln(")")
			.appendln("SELECT")
			//.appendln(getDQLColumns().stream().map(this::prefixTab).collect(Collectors.joining(",\n")))
			.appendln("*")	// DQL columns may contain something like "folder_content_date = now() AS _inserted"
			.appendln("FROM _inserted_row")
			.appendln("UNION  ALL")
			.appendln("SELECT")	// due to LIMIT below, this SELECT will be executed only if INSERT didn't return anything
			.appendln(getDQLColumns().stream().map(this::toTableColumn).map(this::prefixTab).collect(Collectors.joining(",\n")))
			.append("FROM ").appendln(getTableName())
			.append("JOIN _data ON ").appendln(upsertWhereColumns.stream().map(column -> String.format("%s = _data.%s", toTableColumn(column), column)).collect(Collectors.joining(" AND ")))
			.appendln("LIMIT 1")
		;

		return builder.toString();

	}

	@Override
	protected String buildOnConflictExpression() {

		if (upsertSetColumns.isEmpty())
			return "ON CONFLICT DO NOTHING\n";

		if (StringUtils.isBlank(conflictTarget))
			throw new IllegalStateException("UPSERT must have conflict target defined");

		if (upsertWhereColumns.isEmpty())
			throw new IllegalStateException("UPSERT must have at least one WHERE column defined");

		TextStringBuilder builder = new TextStringBuilder();

		builder
			.append("ON CONFLICT (").append(conflictTarget).appendln(") DO UPDATE SET")
			.appendln(upsertSetColumns.stream().map(column -> String.format("%1$s = EXCLUDED.%1$s", column)).map(this::prefixTab).collect(Collectors.joining(",\n")))
			.append("WHERE ").appendln(upsertWhereColumns.stream().map(column -> String.format("%s = EXCLUDED.%s", toTableColumn(column), column)).collect(Collectors.joining(" AND ")))
		;

		return builder.toString();

	}

	@Override
	protected UpsertStatement self() { return this; }

	private String conflictTarget;
	private List<String> upsertSetColumns = new ArrayList<>();
	private List<String> upsertWhereColumns = new ArrayList<>();

}
