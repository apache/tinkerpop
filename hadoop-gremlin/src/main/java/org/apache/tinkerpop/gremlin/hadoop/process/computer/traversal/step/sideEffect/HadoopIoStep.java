/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.hadoop.process.computer.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONOutputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoOutputFormat;
import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.clone.CloneVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.VertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.IO;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.ReadWriting;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.util.OlapClassLoadingPolicy;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.util.OlapConfigKeyPolicy;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;

/**
 * An OLAP oriented step for doing IO operations with {@link GraphTraversalSource#io(String)} which uses the
 * {@link CloneVertexProgram} for its implementation. Standard Hadoop OLAP configurations can be passed using the
 * {@link GraphTraversal#with(String, Object)} step modulator: in a trusted deployment (see
 * {@link OlapClassLoadingPolicy#TRUSTED}) all options aside from those in {@link IO} are transferred. By default a
 * traversal is treated as untrusted, in which case reader/writer selection is restricted to approved formats and other
 * {@code with(k,v)} options are not transferred into the graph configuration, so a remote caller cannot drive
 * reflective class loading, script execution, or native deserialization through OLAP IO.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class HadoopIoStep extends VertexProgramStep implements ReadWriting {

    private Parameters parameters = new Parameters();
    private Mode mode = Mode.UNSET;
    private String file;

    public HadoopIoStep(final Traversal.Admin traversal, final String file) {
        super(traversal);
        this.file = file;
    }

    @Override
    public void setMode(final Mode mode) {
        this.mode = mode;
    }

    @Override
    public Mode getMode() {
        return mode;
    }

    @Override
    public String getFile() {
        return file;
    }

    @Override
    public void configure(final Object... keyValues) {
        this.parameters.set(null, keyValues);
    }

    @Override
    public Parameters getParameters() {
        return parameters;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, new GraphFilter(this.computer));
    }

    @Override
    public CloneVertexProgram generateProgram(final Graph graph, final Memory memory) {
        if (mode == Mode.UNSET)
            throw new IllegalStateException("IO mode was not set to read() or write()");
        else if (mode == Mode.READING)
            configureForRead(graph);
        else if (mode == Mode.WRITING)
            configureForWrite(graph);
        else
            throw new IllegalStateException("Invalid ReadWriting.Mode configured in IoStep: " + mode.name());

        return CloneVertexProgram.build().create(graph);
    }

    @Override
    public HadoopIoStep clone() {
        return (HadoopIoStep) super.clone();
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    private void configureForRead(final Graph graph) {
        final boolean trusted = OlapClassLoadingPolicy.isTrusted(graph.configuration());
        final String reader = parameters.get(IO.reader, this::detectReader).get(0);
        final String inputFormatClassName = resolveFormat(graph, reader, trusted, GryoInputFormat.class, GraphSONInputFormat.class);

        graph.configuration().setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, inputFormatClassName);
        graph.configuration().setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, file);

        addParametersToConfiguration(graph);
    }

    private void configureForWrite(final Graph graph) {
        final boolean trusted = OlapClassLoadingPolicy.isTrusted(graph.configuration());
        final String writer = parameters.get(IO.writer, this::detectWriter).get(0);
        final String outputFormatClassName = resolveFormat(graph, writer, trusted, GryoOutputFormat.class, GraphSONOutputFormat.class);

        graph.configuration().setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, outputFormatClassName);
        graph.configuration().setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, file);

        addParametersToConfiguration(graph);
    }

    /**
     * Resolves a reader/writer token to a concrete format class name. A built-in keyword maps to the corresponding
     * built-in format; GraphML is unsupported for OLAP; any other value is an explicit class name that, in untrusted
     * mode, must resolve to an approved format. Centralizing this keeps the untrusted approval check in a single place
     * for both the read and write paths.
     */
    private String resolveFormat(final Graph graph, final String nameOrKeyword, final boolean trusted,
                                 final Class<?> gryoFormat, final Class<?> graphsonFormat) {
        if (nameOrKeyword.equals(IO.graphson))
            return graphsonFormat.getName();
        else if (nameOrKeyword.equals(IO.gryo))
            return gryoFormat.getName();
        else if (nameOrKeyword.equals(IO.graphml))
            throw new IllegalStateException("GraphML is not a supported file format for OLAP");
        else {
            // an explicit format class name -- in untrusted mode it must resolve to an approved format
            if (!trusted && !approvedFormats(graph).isApproved(nameOrKeyword))
                throw new IllegalArgumentException(formatRejection(nameOrKeyword));
            return nameOrKeyword;
        }
    }

    /**
     * Copies configurations from values passed using {@link GraphTraversal#with(String, Object)} into the graph
     * configuration. Each key is gated by {@link OlapConfigKeyPolicy}: in untrusted (default) mode a remote traversal
     * may set only keys the operator approved via {@link OlapConfigKeyPolicy#APPROVED_GRAPH_CONFIG_KEYS} (trust-boundary
     * keys are never settable); trusted deployments (see {@link OlapClassLoadingPolicy#TRUSTED}) copy all options. This
     * keeps {@code io().with()} consistent with a graph computer's {@code configure()} while each surface keeps its own
     * operator allow-list. The open reflective config surface (formats, computers, serializers, codecs, filesystem
     * implementations, scripts) therefore stays closed to untrusted input unless an operator explicitly opens a key.
     */
    private void addParametersToConfiguration(final Graph graph) {
        parameters.getRaw(IO.reader, IO.writer, IO.registry).entrySet().forEach(kv -> {
            final String key = kv.getKey().toString();
            OlapConfigKeyPolicy.checkConfigKeyPermitted(graph.configuration(), key,
                    Collections.emptySet(), OlapConfigKeyPolicy.APPROVED_GRAPH_CONFIG_KEYS);
            if (kv.getValue().size() == 1)
                graph.configuration().setProperty(key, kv.getValue().get(0));
            else {
                // reset the default configuration with the first option then add to that for List options
                for (int ix = 0; ix < kv.getValue().size(); ix++) {
                    if (ix == 0)
                        graph.configuration().setProperty(key, kv.getValue().get(ix));
                    else
                        graph.configuration().addProperty(key, kv.getValue().get(ix));
                }
            }
        });
    }

    /**
     * The formats a remote (untrusted) traversal may name explicitly: the built-in Gryo/GraphSON formats, any format
     * class the operator already declared in trusted graph configuration, and any class the operator approved via
     * {@link OlapClassLoadingPolicy#APPROVED_CLASSES}. It is seeded only from the trusted base configuration and must be
     * built before any {@code with(k,v)} values are merged into the configuration, so an attacker-supplied reader/writer
     * value can never approve itself.
     */
    private static OlapClassLoadingPolicy approvedFormats(final Graph graph) {
        return OlapClassLoadingPolicy.build()
                .approve(GryoInputFormat.class, GraphSONInputFormat.class, GryoOutputFormat.class, GraphSONOutputFormat.class)
                .approveFromConfigValues(graph.configuration(),
                        Constants.GREMLIN_HADOOP_GRAPH_READER, Constants.GREMLIN_HADOOP_GRAPH_WRITER)
                .approveFrom(graph.configuration())
                .create();
    }

    private static String formatRejection(final String name) {
        return String.format(
                "The format '%s' is not an approved OLAP format for a remote traversal. Use a built-in format ('%s'/'%s'), " +
                "declare it in trusted graph configuration, or add it to '%s'.",
                name, IO.gryo, IO.graphson, OlapClassLoadingPolicy.APPROVED_CLASSES);
    }

    private String detectReader() {
        if (file.endsWith(".kryo"))
            return GryoInputFormat.class.getName();
        else if (file.endsWith(".json"))
            return GraphSONInputFormat.class.getName();
        else if (file.endsWith(".xml"))
            throw new IllegalStateException("GraphML is not a supported file format for OLAP");
        else
            throw new IllegalStateException("Could not detect the file format - specify the reader explicitly or rename file with a standard extension");
    }

    private String detectWriter() {
        if (file.endsWith(".kryo"))
            return GryoOutputFormat.class.getName();
        else if (file.endsWith(".json"))
            return GraphSONOutputFormat.class.getName();
        else if (file.endsWith(".xml"))
            throw new IllegalStateException("GraphML is not a supported file format for OLAP");
        else
            throw new IllegalStateException("Could not detect the file format - specify the reader explicitly or rename file with a standard extension");
    }
}
