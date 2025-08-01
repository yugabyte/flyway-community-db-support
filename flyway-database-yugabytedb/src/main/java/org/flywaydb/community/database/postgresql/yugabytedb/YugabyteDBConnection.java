/*-
 * ========================LICENSE_START=================================
 * flyway-database-yugabytedb
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

package org.flywaydb.community.database.postgresql.yugabytedb;

import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.database.postgresql.PostgreSQLConnection;

import java.util.concurrent.Callable;

public class YugabyteDBConnection extends PostgreSQLConnection {

    YugabyteDBConnection(YugabyteDBDatabase database, java.sql.Connection connection) {
        super(database, connection);
    }

    @Override
    public Schema getSchema(String name) {
        return new YugabyteDBSchema(jdbcTemplate, (YugabyteDBDatabase) database, name);
    }

    @Override
    public <T> T lock(Table table, Callable<T> callable) {
        return new YugabyteDBExecutionTemplate(jdbcTemplate, table.toString()).execute(callable);
    }
}
