package com.parknav.common.sql.where;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.parknav.common.sql.QueryBuilder;
import com.parknav.common.sql.SimpleTerm;

public class SimpleWhereTerm extends WhereTerm {

	public SimpleWhereTerm add(SimpleTerm.Builder builder) {
		return add(new SimpleTerm(builder));
	}
	
	public SimpleWhereTerm add(SimpleTerm.Builder builder, SimpleTerm.Setter setter) {
		return add(new SimpleTerm(builder, setter));
	}
	
	public SimpleWhereTerm add(SimpleTerm term) {
		terms.add(term);
		return this;
	}

	@Override
	public String build() {

		QueryBuilder builder = getQueryBuilder();

		for (SimpleTerm term : terms)
			term.builder().accept(builder);

		return builder.toString();

	}
	
	@Override
	public int setValues(PreparedStatement statement, int offset) throws SQLException {

		for (SimpleTerm term : terms)
			if (term.setter() != null)
				offset = term.setter().apply(statement, offset);
		
		return offset;

	}
	
	private final List<SimpleTerm> terms = new ArrayList<>();

}
