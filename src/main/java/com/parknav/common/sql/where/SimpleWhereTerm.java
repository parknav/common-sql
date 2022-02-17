package com.parknav.common.sql.where;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.parknav.common.sql.QueryBuilder;

public class SimpleWhereTerm extends WhereTerm {

	public static class Term {
		
		@FunctionalInterface
		public interface Builder {
			void accept(QueryBuilder builder);
		}
		
		@FunctionalInterface
		public interface Setter {
			Integer apply(PreparedStatement statement, int offset) throws SQLException;
		}

		public Term(Builder builder) {
			this(builder, null);
		}
		
		public Term(Builder builder, Setter setter) {
			this.builder = builder;
			this.setter = setter;
		}
		
		private final Builder builder; 
		private final Setter setter;
		
	}

	public SimpleWhereTerm add(Term.Builder builder) {
		return add(new Term(builder));
	}
	
	public SimpleWhereTerm add(Term.Builder builder, Term.Setter setter) {
		return add(new Term(builder, setter));
	}
	
	public SimpleWhereTerm add(Term term) {
		terms.add(term);
		return this;
	}

	@Override
	public String build() {

		QueryBuilder builder = getQueryBuilder();

		for (Term term : terms)
			term.builder.accept(builder);

		return builder.toString();

	}
	
	@Override
	public int setValues(PreparedStatement statement, int offset) throws SQLException {

		for (Term term : terms)
			if (term.setter != null)
				offset = term.setter.apply(statement, offset);
		
		return offset;

	}
	
	private final List<Term> terms = new ArrayList<>();

}
