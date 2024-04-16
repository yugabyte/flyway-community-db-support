package org.flywaydb.community.database.postgresql.yugabytedb;

import lombok.CustomLog;
import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.exception.FlywaySqlException;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

import java.sql.*;
import java.util.HashMap;
import java.util.concurrent.Callable;

@CustomLog
public class YugabyteDBExecutionTemplate {

    private final Configuration configuration;
    private final JdbcTemplate jdbcTemplate;
    private final String tableName;
    private final HashMap<String, Boolean> tableEntries = new HashMap<>();

    private final long MAX_FLYWAY_OP_DURATION_SEC = 30;


    YugabyteDBExecutionTemplate(Configuration configuration, JdbcTemplate jdbcTemplate, String tableName) {
        this.configuration = configuration;
        this.jdbcTemplate = jdbcTemplate;
        this.tableName = tableName;
    }

    public <T> T execute(Callable<T> callable) {
        Exception error = null;
        try {
            lock();
            return callable.call();
        } catch (RuntimeException e) {
            error = e;
            throw e;
        } catch (Exception e) {
            error = e;
            throw new FlywayException(e);
        } finally {
            unlock(error);
        }
    }

    private void lock() {
        Exception exception = null;
        boolean txStarted = false;
        Statement statement = null;
        try {
            statement = jdbcTemplate.getConnection().createStatement();

            if (!tableEntries.containsKey(tableName)) {
                try {
                    statement.executeUpdate("INSERT INTO " + YugabyteDBDatabase.LOCK_TABLE_NAME + " VALUES ('" + tableName + "', 'false', NOW())");
                    tableEntries.put(tableName, true);
                    LOG.info(Thread.currentThread().getName() + "> Inserted a record for " + tableName);
                } catch (SQLException e) {
                    if ("23505".equals(e.getSQLState())) {
                        // 23505 == UNIQUE_VIOLATION
                        LOG.debug(Thread.currentThread().getName() + "> Table entry already added for " + tableName);
                    } else {
                        throw new FlywaySqlException("Could not initialize lock for table " + YugabyteDBDatabase.LOCK_TABLE_NAME, e);
                    }
                }
            }

            boolean locked;
            String selectForUpdate = "SELECT locked, last_updated FROM " + YugabyteDBDatabase.LOCK_TABLE_NAME + " WHERE table_name = '" + tableName + "' FOR UPDATE";
            String updateLocked = "UPDATE " + YugabyteDBDatabase.LOCK_TABLE_NAME + " SET locked = true, last_updated = NOW() WHERE table_name = '" + tableName + "'";
            do {
                statement.execute("BEGIN");
                txStarted = true;
                ResultSet rs = statement.executeQuery(selectForUpdate);
                if (rs.next()) {
                    locked = rs.getBoolean("locked");
                    Timestamp ts = rs.getTimestamp("last_updated");

                    if (locked) {
                        // Was it too long ago?
                        if ((System.currentTimeMillis() - ts.getTime()) > (MAX_FLYWAY_OP_DURATION_SEC * 1000)) {
                            LOG.warn("Some other Flyway operation is in progress for > " + MAX_FLYWAY_OP_DURATION_SEC
                                    + "s, continuing without waiting for it to get over");
                            statement.executeUpdate(updateLocked);
                            break;
                        }
                        // End transaction and retry after a sleep
                        statement.execute("COMMIT");
                        txStarted = false;
                        try {
                            LOG.info(Thread.currentThread().getName() + "> Another Flyway operation is in progress. Allowing it to complete");
                            Thread.sleep(1000);
                        } catch (InterruptedException ie) {
                            throw new FlywayException("Interrupted while waiting to get a lock: " + ie);
                        }
                    } else {
                        LOG.debug(Thread.currentThread().getName() + "> Setting locked = true");
                        statement.executeUpdate(updateLocked);
                        break;
                    }
                } else {
                    // For some reason the record was not found
                    tableEntries.remove(tableName);
                    throw new FlywayException("Unable to perform lock action as " + YugabyteDBDatabase.LOCK_TABLE_NAME + " is not initialized");
                }
            } while (locked);
        } catch (SQLException e) {
            LOG.error(Thread.currentThread().getName() + "> Unable to perform lock action", e);
            exception = new FlywaySqlException("Unable to perform lock action", e);
            throw (FlywaySqlException) exception;
        } finally {
            if (txStarted) {
                try {
                    statement.execute("COMMIT");
                    LOG.debug(Thread.currentThread().getName() + "> Completed the tx to set locked = true");
                } catch (SQLException e) {
                    if (exception == null) {
                        throw new FlywaySqlException("Failed to commit the tx to set locked = true", e);
                    }
                    LOG.error(Thread.currentThread().getName() + "> Failed to commit the tx to set locked = true", e);
                }
            }
        }
    }

    private void unlock(Exception rethrow) {
        Statement statement = null;
        try {
            statement = jdbcTemplate.getConnection().createStatement();
            statement.execute("BEGIN");
            ResultSet rs = statement.executeQuery("SELECT locked FROM " + YugabyteDBDatabase.LOCK_TABLE_NAME + " WHERE table_name = '" + tableName + "' FOR UPDATE");

            if (rs.next()) {
                boolean locked = rs.getBoolean("locked");
                if (locked) {
                    statement.executeUpdate("UPDATE " + YugabyteDBDatabase.LOCK_TABLE_NAME + " SET locked = false, last_updated = NOW() WHERE table_name = '" + tableName + "'");
                } else {
                    // Unexpected. This may happen only when callable took too long to complete
                    // and another thread forcefully reset it.
                    String msg = "Unlock failed but the Flyway operation may have succeeded. Check your Flyway operation before re-trying";
                    LOG.warn(Thread.currentThread().getName() + "> " + msg);
                    if (rethrow == null) {
                        throw new FlywayException(msg);
                    }
                }
            }
        } catch (SQLException e) {
            if (rethrow == null) {
                rethrow = new FlywayException("Unable to perform unlock action", e);
                throw (FlywaySqlException) rethrow;
            }
            LOG.warn("Unable to perform unlock action " + e);
        } finally {
            try {
                statement.execute("COMMIT");
            } catch (SQLException e) {
                if (rethrow == null) {
                    throw new FlywaySqlException("Failed to commit unlock action", e);
                }
                LOG.error("Failed to commit unlock action", e);
            }
        }
    }

}
