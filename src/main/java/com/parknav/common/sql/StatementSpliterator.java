package com.parknav.common.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Spliterator for iterating {@link ResultSet}s.
 *
 * @param <T> type of objects being deserialized
 */
public class StatementSpliterator<T> implements Spliterator<T> {

	public StatementSpliterator(PreparedStatement statement, Function<ResultSet, T> resolver) {

		this.statement = statement;
		this.resolver = resolver;

		try {
			resultSet = statement.executeQuery();
		} catch (SQLException e) {
			close();
			throw new RuntimeException("Error opening ResultSet", e);
		}

	}

	@Override
	public Spliterator<T> trySplit() {
		return null;	// we don't support splitting stream (parallel operations)
	}

	@Override
	public long estimateSize() {
		return Long.MAX_VALUE;	// too expensive to compute
	}

	@Override
	public int characteristics() {
		return DISTINCT | IMMUTABLE | NONNULL | ORDERED;
	}

	@Override
	public boolean tryAdvance(Consumer<? super T> action) {

		try {

			if (!resultSet.next())
				return false;

			T object = resolver.apply(resultSet);

			action.accept(object);

			return true;

		} catch (SQLException e) {
			close();
			throw new RuntimeException("Error reading ResultSet", e);
		}

	}

	private void close() {
		try {
			if (resultSet != null)
				resultSet.close();
			if (statement != null)
				statement.close();
		} catch (SQLException e) {
			throw new RuntimeException("Error closing spliterator", e);
		}
	}

	@SuppressWarnings("unused")
	private static final Logger Log = LoggerFactory.getLogger(StatementSpliterator.class);

	private final PreparedStatement statement;
	private final ResultSet resultSet;
	private final Function<ResultSet, T> resolver;

}
