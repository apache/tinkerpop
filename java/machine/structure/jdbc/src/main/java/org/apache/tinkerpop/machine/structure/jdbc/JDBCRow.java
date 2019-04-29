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
import org.apache.tinkerpop.machine.structure.util.J2Tuple;
import org.apache.tinkerpop.machine.structure.util.T2Tuple;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
final class JDBCRow<V> implements TRow<V> {

    private final ResultSet rows;
    private final int rowId;

    JDBCRow(final ResultSet rows, final int rowId) {
        this.rows = rows;
        this.rowId = rowId;

    }

    @Override
    public boolean has(final String key) {
        try {
            this.rows.absolute(this.rowId);
            this.rows.findColumn(key);
            return true;
        } catch (final SQLException e) {
            return false;
        }
    }

    @Override
    public V value(final String key) {
        try {
            this.rows.absolute(this.rowId);
            return (V) this.rows.getObject(key);
        } catch (final SQLException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public void set(final String key, final V value) {
        try {
            this.rows.absolute(this.rowId);
            this.rows.updateObject(key, value);
        } catch (final SQLException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public void add(final String key, final V value) {
        try {
            this.rows.absolute(this.rowId);
            Object v = this.rows.getObject(key);
            if (v instanceof List)
                ((List) v).add(value);
            else {
                v = new ArrayList<>();
                ((ArrayList) v).add(value);
            }
            this.rows.updateObject(key, v);
        } catch (final SQLException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public void remove(final String key) {
        try {
            this.rows.absolute(this.rowId);
            this.rows.updateObject(key, null);
        } catch (final SQLException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public Iterator<T2Tuple<String, V>> entries() {
        try {
            this.rows.absolute(this.rowId);
            return new Iterator<>() {

                int column = 1;

                @Override
                public boolean hasNext() {
                    try {
                        return rows.getMetaData().getColumnCount() + 1 != column;
                    } catch (final SQLException e) {
                        throw new RuntimeException(e.getMessage(), e);
                    }
                }

                @Override
                public T2Tuple<String, V> next() {
                    try {
                        final J2Tuple<String, V> temp = new J2Tuple<>(rows.getMetaData().getColumnName(column), (V) rows.getObject(column));
                        column++;
                        return temp;
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
        try {
            return "<row#" + this.rows.getMetaData().getTableName(1) + ":" + this.rowId + ">";
        } catch (final SQLException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
