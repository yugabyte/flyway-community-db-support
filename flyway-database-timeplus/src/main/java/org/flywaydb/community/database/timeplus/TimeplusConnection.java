/*-
 * ========================LICENSE_START=================================
 * flyway-database-timeplus
 * ========================================================================
 * Copyright (C) 2010 - 2024 Red Gate Software Ltd
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

package org.flywaydb.community.database.timeplus;

import org.flywaydb.core.internal.database.base.Connection;

import java.sql.SQLException;
import java.util.Optional;

public class TimeplusConnection extends Connection<TimeplusDatabase> {
    private static final String DEFAULT_CATALOG_TERM = "database";

    TimeplusConnection(TimeplusDatabase database, java.sql.Connection connection) {
        super(database, connection);
    }

    @Override
    protected String getCurrentSchemaNameOrSearchPath() throws SQLException {
        var jdbcConnection = getJdbcTemplate().getConnection();
        var currentSchema = useCatalog(jdbcConnection) ?
                jdbcConnection.getCatalog() : jdbcConnection.getSchema();

        return Optional.ofNullable(currentSchema).map(database::unQuote).orElse(null);
    }

    @Override
    public void doChangeCurrentSchemaOrSearchPathTo(String schema) throws SQLException {
        // databaseTerm is catalog since driver version 0.5.0
        // https://github.com/Timeplus/timeplus-java/issues/1273 & https://github.com/dbeaver/dbeaver/issues/19383
        // For compatibility with old libraries, ((TimeplusConnection) getJdbcConnection()).useCatalog() should be checked
        var jdbcConnection = getJdbcTemplate().getConnection();

        if (useCatalog(jdbcConnection)) {
            jdbcConnection.setCatalog(schema);
        } else {
            jdbcConnection.setSchema(schema);
        }
    }

    protected boolean useCatalog(java.sql.Connection jdbcConnection) throws SQLException {
        return DEFAULT_CATALOG_TERM.equals(jdbcConnection.getMetaData().getCatalogTerm());
    }

    @Override
    public TimeplusSchema getSchema(String name) {
        return new TimeplusSchema(jdbcTemplate, database, name);
    }
}
