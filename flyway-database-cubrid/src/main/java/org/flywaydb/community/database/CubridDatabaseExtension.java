/*-
 * ========================LICENSE_START=================================
 * flyway-database-cubrid
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

package org.flywaydb.community.database;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.extensibility.PluginMetadata;
import org.flywaydb.core.internal.util.FileUtils;

public class CubridDatabaseExtension implements PluginMetadata {

    public static String readVersion() {
        try {
            return FileUtils.copyToString(
                Objects.requireNonNull(
                    CubridDatabaseExtension.class.getClassLoader().getResourceAsStream(
                        "org/flywaydb/community/database/cubrid/version.txt")),
                StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new FlywayException("Unable to read extension version: " + e.getMessage(), e);
        }
    }

    @Override
    public String getDescription() {
        return "Community-contributed CUBRID database support extension " + readVersion()
            + " by Redgate";
    }
}
