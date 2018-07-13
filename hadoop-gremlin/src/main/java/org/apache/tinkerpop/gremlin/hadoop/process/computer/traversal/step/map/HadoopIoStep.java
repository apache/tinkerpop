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
package org.apache.tinkerpop.gremlin.hadoop.process.computer.traversal.step.map;

import org.apache.hadoop.mapred.InputFormat;
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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.ReadWriting;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLReader;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONReader;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.lang.reflect.Method;

/**
 * An OLAP oriented step for doing IO operations with {@link GraphTraversalSource#io(String)} which uses the
 * {@link CloneVertexProgram} for its implementation.
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
        // TODO: probably should write to the Configuration selectively - no need for actual Parameters?????????
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
        final String inputFormatClassNameOrKeyword = parameters.get(IO.reader, this::detectReader).get(0);
        String inputFormatClassName;
        if (inputFormatClassNameOrKeyword.equals(IO.graphson))
            inputFormatClassName = GraphSONInputFormat.class.getName();
        else if (inputFormatClassNameOrKeyword.equals(IO.gryo))
            inputFormatClassName = GryoInputFormat.class.getName();
        else if (inputFormatClassNameOrKeyword.equals(IO.graphml))
            throw new IllegalStateException("GraphML is not a supported file format for OLAP");
        else
            inputFormatClassName = inputFormatClassNameOrKeyword;

        graph.configuration().setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, inputFormatClassName);
        graph.configuration().setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, file);
    }

    private void configureForWrite(final Graph graph) {
        final String outputFormatClassNameOrKeyword = parameters.get(IO.writer, this::detectWriter).get(0);
        String outputFormatClassName;
        if (outputFormatClassNameOrKeyword.equals(IO.graphson))
            outputFormatClassName = GraphSONOutputFormat.class.getName();
        else if (outputFormatClassNameOrKeyword.equals(IO.gryo))
            outputFormatClassName = GryoOutputFormat.class.getName();
        else if (outputFormatClassNameOrKeyword.equals(IO.graphml))
            throw new IllegalStateException("GraphML is not a supported file format for OLAP");
        else
            outputFormatClassName = outputFormatClassNameOrKeyword;
        
        graph.configuration().setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, outputFormatClassName);
        graph.configuration().setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, file);
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
