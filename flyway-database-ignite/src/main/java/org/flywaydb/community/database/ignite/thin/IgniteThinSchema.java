/*-
 * ========================LICENSE_START=================================
 * flyway-database-ignite
 * ========================================================================
 * Copyright (C) 2010 - 2025 Red Gate Software Ltd
 * ========================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */

package org.flywaydb.community.database.ignite.thin;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.flywaydb.core.api.logging.Log;
import org.flywaydb.core.api.logging.LogFactory;
import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;
import org.flywaydb.core.internal.util.StringUtils;

/**
 * Apache Ignite implementation of Schema.
 */
public class IgniteThinSchema extends Schema<IgniteThinDatabase, IgniteThinTable> {
    private static final Log LOG = LogFactory.getLog(IgniteThinSchema.class);

    /**
     * Creates a new Ignite schema.
     *
     * @param jdbcTemplate The Jdbc Template for communicating with the DB.
     * @param database The database-specific support.
     * @param name The name of the schema.
     */
    IgniteThinSchema(JdbcTemplate jdbcTemplate, IgniteThinDatabase database, String name) {
        super(jdbcTemplate, database, name);
    }

    @Override
    protected boolean doExists() throws SQLException {
        return jdbcTemplate.queryForInt("SELECT COUNT(*) FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME=?", name) > 0;
    }

    @Override
    protected boolean doEmpty() {
        return allTables().length == 0;
    }

    @Override
    protected void doCreate() throws SQLException {
        jdbcTemplate.execute("CREATE SCHEMA " + database.quote(name)); //Unsupported now by Ignite
    }

    @Override
    protected void doDrop() throws SQLException {
        jdbcTemplate.execute("DROP SCHEMA " + database.quote(name));  //Unsupported now by Ignite
    }

    @Override
    protected void doClean() throws SQLException {
        for (Table table : allTables()) {
            table.drop();
        }

        List<String> sequenceNames = listObjectNames("SEQUENCE", "IS_GENERATED = false");
        for (String statement : generateDropStatements("SEQUENCE", sequenceNames)) {
            jdbcTemplate.execute(statement);
        }

        List<String> constantNames = listObjectNames("CONSTANT", "");
        for (String statement : generateDropStatements("CONSTANT", constantNames)) {
            jdbcTemplate.execute(statement);
        }

        List<String> aliasNames = jdbcTemplate.queryForStringList(
                "SELECT ALIAS_NAME FROM INFORMATION_SCHEMA.FUNCTION_ALIASES WHERE ALIAS_SCHEMA = ?", name);
        for (String statement : generateDropStatements("ALIAS", aliasNames)) {
            jdbcTemplate.execute(statement);
        }

        List<String> domainNames = listObjectNames("DOMAIN", "");
        if (!domainNames.isEmpty()) {
            if (name.equals(database.getMainConnection().getCurrentSchema().getName())) {
                for (String statement : generateDropStatementsForCurrentSchema("DOMAIN", domainNames)) {
                    jdbcTemplate.execute(statement);
                }
            } else {
                LOG.error("Unable to drop DOMAIN objects in schema " + database.quote(name));
            }
        }
    }

    /**
     * Generate the statements for dropping all the objects of this type in this schema.
     *
     * @param objectType The type of object to drop (Sequence, constant, ...)
     * @param objectNames The names of the objects to drop.
     * @return The list of statements.
     */
    private List<String> generateDropStatements(String objectType, List<String> objectNames) {
        List<String> statements = new ArrayList<>();
        for (String objectName : objectNames) {
            String dropStatement =
                    "DROP " + objectType + database.quote(name, objectName);

            statements.add(dropStatement);
        }
        return statements;
    }

    /**
     * Generate the statements for dropping all the objects of this type in the current schema.
     *
     * @param objectType The type of object to drop (Sequence, constant, ...)
     * @param objectNames The names of the objects to drop.
     * @return The list of statements.
     */
    private List<String> generateDropStatementsForCurrentSchema(String objectType, List<String> objectNames) {
        List<String> statements = new ArrayList<>();
        for (String objectName : objectNames) {
            String dropStatement =
                    "DROP " + objectType + database.quote(objectName);

            statements.add(dropStatement);
        }
        return statements;
    }

    @Override
    protected IgniteThinTable[] doAllTables() throws SQLException {
        List<String> tableNames = listObjectNames("TABLE", "TABLE_TYPE = 'TABLE' AND TABLE_NAME !='__T0'");

        IgniteThinTable[] tables = new IgniteThinTable[tableNames.size()];
        for (int i = 0; i < tableNames.size(); i++) {
            tables[i] = new IgniteThinTable(jdbcTemplate, database, this, tableNames.get(i));
        }
        return tables;
    }

    /**
     * List the names of the objects of this type in this schema.
     *
     * @param objectType The type of objects to list (Sequence, constant, ...)
     * @param querySuffix Suffix to append to the query to find the objects to list.
     * @return The names of the objects.
     * @throws SQLException when the object names could not be listed.
     */
    private List<String> listObjectNames(String objectType, String querySuffix) throws SQLException {
        String query = "SELECT " + objectType + "_NAME FROM INFORMATION_SCHEMA." + objectType
                + "S WHERE " + objectType + "_SCHEMA = ?";
        if (StringUtils.hasLength(querySuffix)) {
            query += " AND " + querySuffix;
        }

        return jdbcTemplate.queryForStringList(query, name);
    }

    @Override
    public Table getTable(String tableName) {
        return new IgniteThinTable(jdbcTemplate, database, this, tableName);
    }
}
