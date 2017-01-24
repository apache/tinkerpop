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

package org.apache.tinkerpop.gremlin.spark.jsr223;

import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.BindingsCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.LazyBindingsCustomizer;
import org.apache.tinkerpop.gremlin.spark.process.computer.CombineIterator;
import org.apache.tinkerpop.gremlin.spark.process.computer.MapIterator;
import org.apache.tinkerpop.gremlin.spark.process.computer.MemoryAccumulator;
import org.apache.tinkerpop.gremlin.spark.process.computer.ReduceIterator;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkExecutor;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkMemory;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkMessenger;
import org.apache.tinkerpop.gremlin.spark.structure.Spark;
import org.apache.tinkerpop.gremlin.spark.structure.io.InputFormatRDD;
import org.apache.tinkerpop.gremlin.spark.structure.io.InputOutputHelper;
import org.apache.tinkerpop.gremlin.spark.structure.io.InputRDD;
import org.apache.tinkerpop.gremlin.spark.structure.io.InputRDDFormat;
import org.apache.tinkerpop.gremlin.spark.structure.io.OutputFormatRDD;
import org.apache.tinkerpop.gremlin.spark.structure.io.OutputRDD;
import org.apache.tinkerpop.gremlin.spark.structure.io.PersistedInputRDD;
import org.apache.tinkerpop.gremlin.spark.structure.io.PersistedOutputRDD;
import org.apache.tinkerpop.gremlin.spark.structure.io.SparkContextStorage;

import javax.script.Bindings;
import javax.script.SimpleBindings;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class SparkGremlinPlugin extends AbstractGremlinPlugin {

    protected static String NAME = "tinkerpop.spark";

    private static final ImportCustomizer imports = DefaultImportCustomizer.build().addClassImports(
            SparkGraphComputer.class,
            CombineIterator.class,
            MapIterator.class,
            MemoryAccumulator.class,
            ReduceIterator.class,
            SparkExecutor.class,
            SparkGraphComputer.class,
            SparkMemory.class,
            SparkMessenger.class,
            Spark.class,
            InputFormatRDD.class,
            InputOutputHelper.class,
            InputRDD.class,
            InputRDDFormat.class,
            OutputFormatRDD.class,
            OutputRDD.class,
            PersistedInputRDD.class,
            PersistedOutputRDD.class,
            SparkContextStorage.class).create();

    private static final BindingsCustomizer bindings = new LazyBindingsCustomizer(() -> {
        final Bindings bindings = new SimpleBindings();
        bindings.put("spark", SparkContextStorage.open());
        return bindings;
    });

    private static final SparkGremlinPlugin instance = new SparkGremlinPlugin();

    public SparkGremlinPlugin() {
        super(NAME, imports, bindings);
    }

    public static SparkGremlinPlugin instance() {
        return instance;
    }

    @Override
    public boolean requireRestart() {
        return true;
    }
}