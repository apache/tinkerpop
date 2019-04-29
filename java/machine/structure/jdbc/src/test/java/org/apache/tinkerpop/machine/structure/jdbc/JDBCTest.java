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

import org.apache.tinkerpop.language.gremlin.Gremlin;
import org.apache.tinkerpop.language.gremlin.TraversalSource;
import org.apache.tinkerpop.language.gremlin.core.__;
import org.apache.tinkerpop.machine.Machine;
import org.apache.tinkerpop.machine.processor.pipes.PipesProcessor;
import org.apache.tinkerpop.machine.species.LocalMachine;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class JDBCTest {

    @Test
    void doStuff2() throws Exception {
        final Connection connection = DriverManager.getConnection("jdbc:h2:/tmp/test");
        connection.createStatement().execute("CREATE TABLE IF NOT EXISTS people (\n" +
                "    name VARCHAR(255) NOT NULL,\n" +
                "    age TINYINT NOT NULL)");
        connection.createStatement().execute("CREATE TABLE IF NOT EXISTS addresses (\n" +
                "    name VARCHAR(255) NOT NULL,\n" +
                "    city VARCHAR(255) NOT NULL)");
        //connection.createStatement().execute("INSERT INTO people(`name`,`age`) VALUES ('marko',29)");
        //connection.createStatement().execute("INSERT INTO people(`name`,`age`) VALUES ('josh',32)");
        //connection.createStatement().execute("INSERT INTO addresses(`name`,`city`) VALUES ('josh','san jose')");
        //connection.createStatement().execute("INSERT INTO addresses(`name`,`city`) VALUES ('marko','santa fe')");


       /* final TDatabase db = new JDBCDatabase("jdbc:h2:/tmp/test");
        System.out.println(db.has("people"));
        final TTable table = db.value("people");
        for (final TRow<?> row : table) {
            row.entries().forEach(System.out::println);
        }
        */


        ////////

        final Machine machine = LocalMachine.open();
        final TraversalSource<Long> jdbc =
                Gremlin.<Long>traversal(machine).
                        withProcessor(PipesProcessor.class).
                        withStructure(JDBCStructure.class, Map.of(JDBCStructure.JDBC_CONNECTION, "jdbc:h2:/tmp/test"));
        System.out.println(jdbc.db().toList());
        System.out.println(jdbc.db().entries().toList());
        System.out.println(jdbc.db().value("people").toList());
        System.out.println(jdbc.db().values("people").toList());
        System.out.println(jdbc.db().values("people").value("name").toList());
        System.out.println(jdbc.db().values("people").entries().toList());
        System.out.println(jdbc.db().values("people").as("x").db().values("addresses").as("y").has("name", __.path("x").by("name")).path("x", "y").toList());
        System.out.println(jdbc.db().values("people").as("x").db().values("addresses").as("y").has("name", __.path("x").by("name")).path("x", "y").explain().toList());
    }
}
