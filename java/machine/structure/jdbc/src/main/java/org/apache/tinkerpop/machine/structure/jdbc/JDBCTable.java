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

import org.apache.tinkerpop.machine.structure.rdbms.TRow;
import org.apache.tinkerpop.machine.structure.rdbms.TTable;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
final class JDBCTable implements TTable {

    private final Connection connection;
    private final String name;

    JDBCTable(final Connection connection, final String name) {
        this.connection = connection;
        this.name = name;
    }

    @Override
    public Iterator<TRow<?>> iterator() {
        try {
            final ResultSet resultSet = this.connection.createStatement().executeQuery("SELECT * FROM " + this.name);
            return new Iterator<>() {

                private boolean done = false;

                @Override
                public boolean hasNext() {
                    return !this.done;
                }

                @Override
                public TRow<?> next() {
                    try {
                        resultSet.next();
                        this.done = resultSet.isLast();
                        return new JDBCRow<>(resultSet, resultSet.getRow());
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
    public void add(final TRow<?> value) {
        try {
            this.connection.createStatement().executeUpdate("INSERT INTO " + this.name + " ()" + " VALUES (" + value + ")");
        } catch (final SQLException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public void remove(final TRow<?> value) {
        // TODO
    }

    @Override
    public String toString() {
        return "<table#" + this.name + ">";
    }

    // TODO: equals(), hashcode()
}
