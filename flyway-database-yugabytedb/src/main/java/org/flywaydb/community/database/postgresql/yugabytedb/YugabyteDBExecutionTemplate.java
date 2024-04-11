package org.flywaydb.community.database.postgresql.yugabytedb;

import lombok.CustomLog;
import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.exception.FlywaySqlException;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;
import org.flywaydb.core.internal.jdbc.RowMapper;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
            System.out.println("Lock Connection " + jdbcTemplate.getConnection().toString() + ", thread: " + Thread.currentThread().getName());
            lock();
            return callable.call();
        } catch (RuntimeException e) {
            error = e;
            throw e;
        } catch (Exception e) {
            error = e;
            throw new FlywayException(e);
        } finally {
            System.out.println("Unlock Connection " + jdbcTemplate.getConnection().toString() + ", thread: " + Thread.currentThread().getName());
            unlock(error);
        }
    }

    private void lock() {
        boolean txStarted = false;
        try {
            if (!tableEntries.containsKey(tableName)) {
                try {
                    jdbcTemplate.execute("INSERT INTO " + YugabyteDBDatabase.LOCK_TABLE_NAME + " VALUES ('" + tableName + "', 'false', NOW());");
                    tableEntries.put(tableName, true);
                    LOG.warn("Inserted a record for " + tableName);
                } catch (SQLException e) {
                    if ("23505".equals(e.getSQLState())) {
                        // 23505 == UNIQUE_VIOLATION
                        LOG.debug("Table entry already added");
                    } else {
                        throw new FlywaySqlException("Could not initialize lock for table " + YugabyteDBDatabase.LOCK_TABLE_NAME, e);
                    }
                }
            }

            boolean locked;
            do {
                jdbcTemplate.execute("BEGIN;");
                txStarted = true;
                RowMapper<ArrayList> rm = resultSet -> {
                    ArrayList e = new ArrayList();
                    e.add(resultSet.getBoolean("locked"));
                    e.add(resultSet.getTimestamp("last_updated"));
                    return e;
                };
                List r = jdbcTemplate.query("SELECT locked, last_updated FROM " + YugabyteDBDatabase.LOCK_TABLE_NAME + " WHERE table_name = '" + tableName + "' FOR UPDATE;", rm);
                ArrayList row = (ArrayList) r.get(0);
                locked = (Boolean) row.get(0);
                Timestamp ts = (Timestamp) row.get(1);
                if (locked) {
                    // Was it too long ago?
                    if ((System.currentTimeMillis() - ts.getTime()) > (MAX_FLYWAY_OP_DURATION_SEC * 1000)) {
                        LOG.warn("Some other Flyway operation is in-progress for > " + MAX_FLYWAY_OP_DURATION_SEC
                                + "s. Continuing without waiting for it to get over.");
                        jdbcTemplate.execute("UPDATE " + YugabyteDBDatabase.LOCK_TABLE_NAME + " SET locked = true, last_updated = NOW() WHERE table_name = '" + tableName + "';");
                        break;
                    }
                    // End transaction and retry after a sleep
                    jdbcTemplate.execute("COMMIT;");
                    txStarted = false;
                    try {
                        LOG.info(Thread.currentThread().getName() + " Sleeping for a second");
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        throw new FlywayException("Interrupted while waiting to get a lock: " + ie);
                    }
                } else {
                    LOG.info("Setting locked = true ...");
                    jdbcTemplate.execute("UPDATE " + YugabyteDBDatabase.LOCK_TABLE_NAME + " SET locked = true, last_updated = NOW() WHERE table_name = '" + tableName + "';");
                    break;
                }
            } while (locked);
        } catch (SQLException e) {
            throw new FlywaySqlException("Trying to acquire lock failed", e);
        } finally {
            if (txStarted) {
                try {
                    jdbcTemplate.execute("COMMIT;");
                    LOG.info("Completed the tx to set locked = true");
                } catch (SQLException e) {
                    throw new FlywaySqlException("Trying to acquire lock failed", e);
                }
            }
        }
    }

    private void unlock(Exception rethrow) throws FlywaySqlException {
        try {
            jdbcTemplate.execute("BEGIN;");
            boolean locked = jdbcTemplate.queryForBoolean("SELECT locked FROM " + YugabyteDBDatabase.LOCK_TABLE_NAME + " WHERE table_name = '" + tableName + "' FOR UPDATE;");
            if (locked) {
                jdbcTemplate.execute("UPDATE " + YugabyteDBDatabase.LOCK_TABLE_NAME + " SET locked = false, last_updated = NOW() WHERE table_name = '" + tableName + "';");
            } else {
                // Unexpected. Only time this may happen is when callable took too long to complete
                // and another thread forcefully reset it.
                LOG.warn("Unlock failed. Check your Flyway operation");
                throw new FlywayException("Unlock failed. Check your Flyway operation and try again");
            }
            jdbcTemplate.execute("COMMIT;");
        } catch (SQLException e) {
            LOG.error("Commit failed: " + e);
            if (rethrow == null) {
                throw new FlywaySqlException("Commit failed", e);
            }
        }
    }

}
