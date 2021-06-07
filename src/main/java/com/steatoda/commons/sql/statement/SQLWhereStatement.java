package com.steatoda.commons.sql.statement;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.steatoda.commons.sql.where.WhereTerm;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.TextStringBuilder;

public abstract class SQLWhereStatement<S extends SQLTableStatement<S>> extends SQLTableStatement<S> {

	public SQLWhereStatement(String tableName) {
		super(tableName);
	}

	public List<WhereTerm> getWhereTerms() {
		return whereTerms;
	}
	
	public S setWhereTerms(Collection<WhereTerm> whereTerms) {
		if (whereTerms == null)
			throw new NullPointerException();
		this.whereTerms.clear();
		return addWhereTerms(whereTerms);
	}
	
	public S addWhereTerms(WhereTerm... whereTerms) {
		addWhereTerms(Arrays.asList(whereTerms));
		return self();
	}
	
	public S addWhereTerms(Collection<WhereTerm> whereTerms) {
		for (WhereTerm term : whereTerms) {
			term.setStatement(this);
			this.whereTerms.add(term);
		}
		return self();
	}

	/**
	 * Builds whole WHERE expression by joins WHERE clauses from all {@link WhereTerm}s. If non-null, string will be terminated with newline.
	 *
	 * @return built WHERE expression
	 */
	protected String buildWhereExpression() {
		
		if (whereTerms.isEmpty())
			return null;
		
		String expression = getWhereTerms()
			.stream()
			.map(WhereTerm::build)
			.map(StringUtils::stripToNull)	// NOTE strips leading tab and trailing newline, too!
			.filter(Objects::nonNull)
			.map(this::prefixTab)																	// prefix first line
			.map(str -> str.replaceAll("\n", "\n".concat(StringUtils.repeat('\t', getLevel()))))	// prefix each new line
			.collect(Collectors.joining("\n"));

		if (StringUtils.isBlank(expression))
			return null;
		
		return new TextStringBuilder()
			.appendln("WHERE TRUE")
			.appendln(expression)	// StringUtils.stripToNull stripped trailing newline, so re-apply it
			.toString();
		
	}
	
	protected int setWhereValues(PreparedStatement statement, int index) throws SQLException {
		for (WhereTerm term : getWhereTerms())
			index = term.setValues(statement, index);
		return index;
	}

	private List<WhereTerm> whereTerms = new ArrayList<>();
	
}
