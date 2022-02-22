package com.parknav.common.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * Spliterator for iterating {@link ResultSet}s.
 *
 * @param <T> type of objects being deserialized
 */
public class StatementSpliterator<T> implements Spliterator<T> {

	@FunctionalInterface
	public interface Resolver<T> {
		T resolve(ResultSet resultSet) throws SQLException;
	}

	public StatementSpliterator(PreparedStatement statement, Resolver<T> resolver) {

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

			// this method may be invoked even if we previously returned false,
			// so guard us from accessing already closed ResultSet
			if (resultSet.isClosed())
				return false;

			if (!resultSet.next()) {
				close();
				return false;
			}

			T object = resolver.resolve(resultSet);

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
			throw new RuntimeException("Error closing resources", e);
		}
	}

	@SuppressWarnings("unused")
	private static final Logger Log = LoggerFactory.getLogger(StatementSpliterator.class);

	private final PreparedStatement statement;
	private final ResultSet resultSet;
	private final Resolver<T> resolver;

}
