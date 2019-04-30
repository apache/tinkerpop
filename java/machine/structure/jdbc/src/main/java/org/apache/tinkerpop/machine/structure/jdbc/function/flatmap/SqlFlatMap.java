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
package org.apache.tinkerpop.machine.structure.jdbc.function.flatmap;

import org.apache.tinkerpop.machine.bytecode.Instruction;
import org.apache.tinkerpop.machine.coefficient.Coefficient;
import org.apache.tinkerpop.machine.function.AbstractFunction;
import org.apache.tinkerpop.machine.function.initial.Initializing;
import org.apache.tinkerpop.machine.structure.TTuple;
import org.apache.tinkerpop.machine.structure.util.JTuple;
import org.apache.tinkerpop.machine.traverser.Traverser;
import org.apache.tinkerpop.machine.traverser.path.BasicPath;
import org.apache.tinkerpop.machine.traverser.path.Path;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SqlFlatMap<C, S> extends AbstractFunction<C> implements Initializing<C, S, Path> {

    private final Connection connection;
    private final String sqlQuery;
    private final String as1;
    private final String as2;

    public SqlFlatMap(final Coefficient<C> coefficient, final String label, final Connection connection, final String as1, final String as2, final String sqlQuery) {
        super(coefficient, label);
        this.connection = connection;
        this.sqlQuery = sqlQuery;
        this.as1 = as1;
        this.as2 = as2;
    }

    @Override
    public Iterator<Path> apply(final Traverser<C, S> traverser) {
        try {
            final ResultSet resultSet = this.connection.createStatement().executeQuery(this.sqlQuery);
            return new Iterator<>() {
                boolean done = false;

                @Override
                public boolean hasNext() {
                    return !this.done;
                }

                @Override
                public Path next() {
                    try {
                        resultSet.next();
                        int size = resultSet.getMetaData().getColumnCount();
                        final Path row = new BasicPath();
                        final TTuple<String, Object> tuple1 = new JTuple<>();
                        final TTuple<String, Object> tuple2 = new JTuple<>();
                        Set<String> labels = new HashSet<>();
                        boolean stay = true;
                        for (int i = 1; i <= size; i++) {
                            final String temp = resultSet.getMetaData().getColumnLabel(i);
                            stay = stay && labels.add(temp);
                            if (stay) {
                                tuple1.set(resultSet.getMetaData().getColumnName(i), resultSet.getObject(i));
                            } else {
                                tuple2.set(resultSet.getMetaData().getColumnName(i), resultSet.getObject(i));
                            }
                        }
                        row.add(as1, tuple1);
                        row.add(as2, tuple2);
                        this.done = resultSet.isLast();
                        return row;

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
    public SqlFlatMap<C, S> clone() {
        return this; // TODO;
    }

    public static <C, S> SqlFlatMap<C, S> compile(final Instruction<C> instruction) {
        return new SqlFlatMap<>(instruction.coefficient(), instruction.label(), (Connection) instruction.args()[0], (String) instruction.args()[1], (String) instruction.args()[2], (String) instruction.args()[3]);
    }
}
