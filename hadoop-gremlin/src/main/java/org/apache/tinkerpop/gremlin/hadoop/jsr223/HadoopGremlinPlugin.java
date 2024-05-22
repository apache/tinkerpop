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
package org.apache.tinkerpop.gremlin.hadoop.jsr223;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.mapreduce.MapReduceGraphComputer;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopConfiguration;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopEdge;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopElement;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopProperty;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopVertex;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopVertexProperty;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.FileSystemStorage;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONOutputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONRecordReader;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONRecordWriter;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoOutputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoRecordReader;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoRecordWriter;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.script.ScriptInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.script.ScriptOutputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.script.ScriptRecordReader;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.script.ScriptRecordWriter;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.BindingsCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.Customizer;
import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.LazyBindingsCustomizer;

import javax.script.Bindings;
import javax.script.SimpleBindings;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class HadoopGremlinPlugin extends AbstractGremlinPlugin {

    protected static String NAME = "tinkerpop.hadoop";

    private static final BindingsCustomizer bindings;

    private static final ImportCustomizer imports;

    private static final Set<String> appliesTo = Collections.emptySet();

    static {
        try {
            imports = DefaultImportCustomizer.build()
                    .addClassImports(
                            Configuration.class,
                            org.apache.hadoop.hdfs.DFSClient.class,
                            FileSystem.class,
                            ToolRunner.class,
                            IOUtils.class,
                            CodecPool.class,
                            SequenceFileInputFormat.class,
                            SequenceFileOutputFormat.class,
                            Constants.class,
                            HadoopConfiguration.class,
                            HadoopEdge.class,
                            HadoopElement.class,
                            HadoopGraph.class,
                            HadoopProperty.class,
                            HadoopVertex.class,
                            HadoopVertexProperty.class,
                            ConfUtil.class,
                            VertexWritable.class,
                            GraphSONInputFormat.class,
                            GraphSONOutputFormat.class,
                            GraphSONRecordReader.class,
                            GraphSONRecordWriter.class,
                            GryoInputFormat.class,
                            GryoOutputFormat.class,
                            GryoRecordReader.class,
                            GryoRecordWriter.class,
                            ScriptInputFormat.class,
                            ScriptOutputFormat.class,
                            ScriptRecordReader.class,
                            ScriptRecordWriter.class,
                            MapReduceGraphComputer.class).create();

            bindings = new LazyBindingsCustomizer(() -> {
                try {
                    final Bindings bindings = new SimpleBindings();
                    bindings.put("hdfs", FileSystemStorage.open(FileSystem.get(new Configuration())));
                    bindings.put("fs", FileSystemStorage.open(FileSystem.getLocal(new Configuration())));
                    return bindings;
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            });
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private static final HadoopGremlinPlugin plugin = new HadoopGremlinPlugin();

    public HadoopGremlinPlugin() {
        super(NAME, appliesTo, imports, bindings);
    }

    @Override
    public Optional<Customizer[]> getCustomizers(final String scriptEngineName) {
        if (null == System.getenv(Constants.HADOOP_GREMLIN_LIBS))
            HadoopGraph.LOGGER.warn("Be sure to set the environmental variable: " + Constants.HADOOP_GREMLIN_LIBS);
        else
            HadoopGraph.LOGGER.info(Constants.HADOOP_GREMLIN_LIBS + " is set to: " + System.getenv(Constants.HADOOP_GREMLIN_LIBS));

        return super.getCustomizers(scriptEngineName);
    }

    @Override
    public boolean requireRestart() {
        return true;
    }

    public static HadoopGremlinPlugin instance() {
        return plugin;
    }
}