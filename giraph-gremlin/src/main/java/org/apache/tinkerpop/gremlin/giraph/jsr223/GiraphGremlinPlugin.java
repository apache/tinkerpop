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
package org.apache.tinkerpop.gremlin.giraph.jsr223;

import org.apache.tinkerpop.gremlin.giraph.process.computer.EmptyOutEdges;
import org.apache.tinkerpop.gremlin.giraph.process.computer.GiraphComputation;
import org.apache.tinkerpop.gremlin.giraph.process.computer.GiraphGraphComputer;
import org.apache.tinkerpop.gremlin.giraph.process.computer.GiraphMemory;
import org.apache.tinkerpop.gremlin.giraph.process.computer.GiraphMessageCombiner;
import org.apache.tinkerpop.gremlin.giraph.process.computer.GiraphMessenger;
import org.apache.tinkerpop.gremlin.giraph.process.computer.GiraphVertex;
import org.apache.tinkerpop.gremlin.giraph.process.computer.GiraphWorkerContext;
import org.apache.tinkerpop.gremlin.giraph.process.computer.MemoryAggregator;
import org.apache.tinkerpop.gremlin.giraph.process.computer.PassThroughMemory;
import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class GiraphGremlinPlugin extends AbstractGremlinPlugin {
    private static final String MODULE_NAME = "tinkerpop.giraph";
    private static final GiraphGremlinPlugin instance = new GiraphGremlinPlugin();

    public GiraphGremlinPlugin() {
        super(MODULE_NAME, DefaultImportCustomizer.build().addClassImports(
                EmptyOutEdges.class,
                GiraphComputation.class,
                GiraphGraphComputer.class,
                GiraphMemory.class,
                GiraphMessageCombiner.class,
                GiraphMessenger.class,
                GiraphVertex.class,
                GiraphWorkerContext.class,
                MemoryAggregator.class,
                PassThroughMemory.class).create());
    }

    public static GiraphGremlinPlugin instance() {
        return instance;
    }
}