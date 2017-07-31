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
package org.apache.tinkerpop.gremlin.hadoop.structure.io.script;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.apache.tinkerpop.gremlin.jsr223.CachedGremlinScriptEngineManager;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngineManager;
import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.process.computer.util.VertexProgramHelper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Optional;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ScriptRecordReader extends RecordReader<NullWritable, VertexWritable> {

    protected final static String SCRIPT_FILE = "gremlin.hadoop.scriptInputFormat.script";
    protected final static String SCRIPT_ENGINE = "gremlin.hadoop.scriptInputFormat.scriptEngine";
    private final static String GRAPH = "graph";
    private final static String LINE = "line";
    private final static String READ_CALL = "parse(" + LINE + ")";
    private final VertexWritable vertexWritable = new VertexWritable();
    private final LineRecordReader lineRecordReader;
    private final GremlinScriptEngineManager manager = new CachedGremlinScriptEngineManager();

    private ScriptEngine engine;
    private CompiledScript script;

    private GraphFilter graphFilter = new GraphFilter();

    public ScriptRecordReader() {
        this.lineRecordReader = new LineRecordReader();
    }

    @Override
    public void initialize(final InputSplit genericSplit, final TaskAttemptContext context) throws IOException {
        this.lineRecordReader.initialize(genericSplit, context);
        final Configuration configuration = context.getConfiguration();
        if (configuration.get(Constants.GREMLIN_HADOOP_GRAPH_FILTER, null) != null)
            this.graphFilter = VertexProgramHelper.deserialize(ConfUtil.makeApacheConfiguration(configuration), Constants.GREMLIN_HADOOP_GRAPH_FILTER);
        this.engine = manager.getEngineByName(configuration.get(SCRIPT_ENGINE, "gremlin-groovy"));
        final FileSystem fs = FileSystem.get(configuration);
        try (final InputStream stream = fs.open(new Path(configuration.get(SCRIPT_FILE)));
             final InputStreamReader reader = new InputStreamReader(stream)) {
            final String parse = String.join("\n", IOUtils.toString(reader), READ_CALL);
            script = ((Compilable) engine).compile(parse);
        } catch (ScriptException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        while (true) {
            if (!this.lineRecordReader.nextKeyValue()) return false;
            try {
                final Bindings bindings = this.engine.createBindings();
                final StarGraph graph = StarGraph.open();
                bindings.put(GRAPH, graph);
                bindings.put(LINE, this.lineRecordReader.getCurrentValue().toString());
                final StarGraph.StarVertex sv = (StarGraph.StarVertex) script.eval(bindings);
                if (sv != null) {
                    final Optional<StarGraph.StarVertex> vertex = sv.applyGraphFilter(this.graphFilter);
                    if (vertex.isPresent()) {
                        this.vertexWritable.set(vertex.get());
                        return true;
                    }
                }
            } catch (final ScriptException e) {
                throw new IOException(e.getMessage(), e);
            }
        }
    }

    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    @Override
    public VertexWritable getCurrentValue() {
        return this.vertexWritable;
    }

    @Override
    public float getProgress() throws IOException {
        return this.lineRecordReader.getProgress();
    }

    @Override
    public synchronized void close() throws IOException {
        this.lineRecordReader.close();
    }
}
