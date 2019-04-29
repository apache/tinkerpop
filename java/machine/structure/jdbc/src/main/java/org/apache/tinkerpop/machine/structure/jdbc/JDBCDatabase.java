/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.machine.structure.jdbc;

import org.apache.tinkerpop.machine.structure.Structure;
import org.apache.tinkerpop.machine.structure.TSequence;
import org.apache.tinkerpop.machine.structure.table.TDatabase;
import org.apache.tinkerpop.machine.structure.table.TTable;
import org.apache.tinkerpop.machine.structure.util.T2Tuple;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
final class JDBCDatabase implements TDatabase, Structure {

    private final Connection connection;

    JDBCDatabase(final String connectionURI) {
        try {
            this.connection = DriverManager.getConnection(connectionURI);
        } catch (final SQLException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public boolean has(final String key) {
        try {
            final ResultSet result = this.connection.createStatement().executeQuery("SHOW TABLES");
            while (result.next()) {
                if (result.getString(1).equalsIgnoreCase(key))
                    return true;
            }
            return false;
        } catch (final SQLException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public TTable value(final String key) {
        return new JDBCTable(this.connection, key);
    }

    @Override
    public void set(final String key, final TTable value) {

    }

    @Override
    public TSequence<T2Tuple<String, TTable>> entries() {
        return null;
    }

}
