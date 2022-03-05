package com.parknav.common.sql;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SimpleTerm {

	@FunctionalInterface
	public interface Builder {
		void accept(QueryBuilder builder);
	}

	@FunctionalInterface
	public interface Setter {
		Integer apply(PreparedStatement statement, int offset) throws SQLException;
	}

	public SimpleTerm(Builder builder) {
		this(builder, null);
	}

	public SimpleTerm(Builder builder, Setter setter) {
		this.builder = builder;
		this.setter = setter;
	}

	public Builder builder() { return builder; }
	public Setter setter() { return setter; }

	private final Builder builder;
	private final Setter setter;

}
