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
package org.apache.tinkerpop.gremlin.process.computer.bulkdumping;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashSet;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.OUTPUT;
import static org.apache.tinkerpop.gremlin.process.traversal.P.eq;
import static org.apache.tinkerpop.gremlin.structure.Column.keys;
import static org.apache.tinkerpop.gremlin.structure.Column.values;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class BulkExportVertexProgramTest extends AbstractGremlinProcessTest {

    @Test
    @LoadGraphWith(MODERN)
    public void shouldExportCsvFilePrepare() {
        if (graphProvider.getGraphComputer(graph).features().supportsResultGraphPersistCombination(GraphComputer.ResultGraph.NEW, GraphComputer.Persist.EDGES)) {
            g.V().hasLabel("person").match(
                    __.as("person").values("name").as("name"),
                    __.as("person").values("age").as("age"),
                    __.as("person").outE("created").count().as("projects"),
                    __.as("person").coalesce(__.out("created"), __.identity()).as("project"),
                    __.as("project").coalesce(__.in("created"), __.identity()).as("coworker"),
                    __.as("coworker").values("name").as("cname"),
                    __.as("coworker").choose(__.where(eq("person")), __.constant(0), __.constant(1)).as("cc")
            ).dedup("person", "coworker").group().by(__.select("person", "name", "age", "projects")).by(__.select("cc").sum()).unfold().as("kv").
                    select(keys).select("person").as("person").
                    select("kv").select(keys).select("name").as("name").
                    select("kv").select(keys).select("age").as("age").
                    select("kv").select(keys).select("projects").as("projects").
                    select("kv").select(values).as("coworkers").
                    select("person", "name", "age", "projects", "coworkers").
                    program(BulkDumperVertexProgram.build().create(graph)).iterate();
        }
    }

    @Test
    @LoadGraphWith(OUTPUT)
    public void shouldExportCsvFileRun() throws Exception {
        if (graphProvider.getGraphComputer(graph).features().supportsResultGraphPersistCombination(GraphComputer.ResultGraph.NEW, GraphComputer.Persist.EDGES)) {
            graph.compute().program(BulkExportVertexProgram.build().keys("name", "age", "projects", "coworkers").create(graph)).submit().get();

            final Set<String> lines = new HashSet<>();
            lines.add("josh,32,2,2");
            lines.add("marko,29,1,2");
            lines.add("peter,35,1,2");
            lines.add("vadas,27,0,0");

            // TODO: use Storage methods
            final File output = new File(graph.configuration().getString("gremlin.hadoop.outputLocation") + "/~g");
            for (final File f : output.listFiles()) {
                if (f.getName().startsWith("part-")) {
                    try (final BufferedReader reader = new BufferedReader(new FileReader(f))) {
                        reader.lines().forEach(line -> assertTrue(lines.remove(line)));
                    }
                }
            }

            assertEquals(0, lines.size());
        }
    }
}