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

import org.apache.tinkerpop.machine.structure.rdbms.TDatabase;
import org.apache.tinkerpop.machine.structure.rdbms.TTable;
import org.apache.tinkerpop.machine.structure.util.JPair;
import org.apache.tinkerpop.machine.structure.TPair;
import org.apache.tinkerpop.machine.util.IteratorUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class JDBCDatabase implements TDatabase {

    private final Connection connection;

    JDBCDatabase(final String connectionURI) {
        try {
            this.connection = DriverManager.getConnection(connectionURI);
        } catch (final SQLException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public Connection getConnection() {
        return this.connection;
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
    public void remove(final String key) {
        try {
            this.connection.createStatement().execute("DROP TABLE " + key);
        } catch (final SQLException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public int size() {
        return (int) IteratorUtils.count(this);
    }

    @Override
    public Iterator<TPair<String, TTable>> iterator() {
        try {
            final ResultSet result = this.connection.createStatement().executeQuery("SHOW TABLES");
            return new Iterator<>() {
                boolean done = false;

                @Override
                public boolean hasNext() {
                    return !this.done;
                }

                @Override
                public TPair<String, TTable> next() {
                    try {
                        result.next();
                        final String tableName = result.getString(1);
                        final TPair<String, TTable> tuple = new JPair<>(tableName, new JDBCTable(connection, tableName));
                        this.done = result.isLast();
                        return tuple;
                    } catch (final SQLException e) {
                        throw new RuntimeException(e.getMessage(), e);
                    }
                }
            };
        } catch (final SQLException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public String toString() {
        return "<database#" + this.connection + ">";
    }

}
