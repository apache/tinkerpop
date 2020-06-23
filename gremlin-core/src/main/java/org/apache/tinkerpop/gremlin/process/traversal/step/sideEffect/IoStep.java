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
package org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.process.traversal.IO;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.ReadWriting;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.EmptyTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.GraphWriter;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLReader;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLWriter;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONReader;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoWriter;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Handles read and write operations into the {@link Graph}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IoStep<S> extends AbstractStep<S,S> implements ReadWriting {

    private Parameters parameters = new Parameters();
    private boolean first = true;
    private String file;
    private Mode mode = Mode.UNSET;

    public IoStep(final Traversal.Admin traversal, final String file) {
        super(traversal);

        if (null == file || file.isEmpty())
            throw new IllegalArgumentException("file cannot be null or empty");

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
    public Parameters getParameters() {
        return this.parameters;
    }

    @Override
    public void configure(final Object... keyValues) {
        this.parameters.set(null, keyValues);
    }

    @Override
    protected Traverser.Admin<S> processNextStart() {
        if (mode == Mode.UNSET) throw new IllegalStateException("IO mode was not set to read() or write()");
        if (!this.first) throw FastNoSuchElementException.instance();

        this.first = false;
        final File file = new File(this.file);

        if (mode == Mode.READING) {
            if (!file.exists()) throw new IllegalStateException(this.file + " does not exist");
            return read(file);
        } else if (mode == Mode.WRITING) {
            return write(file);
        } else {
            throw new IllegalStateException("Invalid ReadWriting.Mode configured in IoStep: " + mode.name());
        }
    }

    protected Traverser.Admin<S> write(final File file) {
        try (final OutputStream stream = new FileOutputStream(file)) {
            final Graph graph = (Graph) this.traversal.getGraph().get();
            constructWriter().writeGraph(stream, graph);

            return EmptyTraverser.instance();
        } catch (IOException ioe) {
            throw new IllegalStateException(String.format("Could not write file %s from graph", this.file), ioe);
        }
    }

    protected Traverser.Admin<S> read(final File file) {
        try (final InputStream stream = new FileInputStream(file)) {
            final Graph graph = (Graph) this.traversal.getGraph().get();
            constructReader().readGraph(stream, graph);

            return EmptyTraverser.instance();
        } catch (IOException ioe) {
            throw new IllegalStateException(String.format("Could not read file %s into graph", this.file), ioe);
        }
    }

    /**
     * Builds a {@link GraphReader} instance to use. Attempts to detect the file format to be read using the file
     * extension or simply uses configurations provided by the user on the parameters given to the step.
     */
    private GraphReader constructReader() {
        final Object objectOrClass = parameters.get(IO.reader, this::detectFileType).get(0);
        if (objectOrClass instanceof GraphReader)
            return (GraphReader) objectOrClass;
        else if (objectOrClass instanceof String) {
            if (objectOrClass.equals(IO.graphson)) {
                final GraphSONMapper.Builder builder = GraphSONMapper.build();
                detectRegistries().forEach(builder::addRegistry);
                return GraphSONReader.build().mapper(builder.create()).create();
            } else if (objectOrClass.equals(IO.gryo)){
                final GryoMapper.Builder builder = GryoMapper.build();
                detectRegistries().forEach(builder::addRegistry);
                return GryoReader.build().mapper(builder.create()).create();
            } else if (objectOrClass.equals(IO.graphml))
                return GraphMLReader.build().create();
            else {
                try {
                    final Class<?> graphReaderClazz = Class.forName((String) objectOrClass);
                    final Method build = graphReaderClazz.getMethod("build");
                    final GraphReader.ReaderBuilder builder = (GraphReader.ReaderBuilder) build.invoke(null);
                    return builder.create();
                } catch (Exception ex) {
                    throw new IllegalStateException(String.format("Could not construct the specified GraphReader of %s", objectOrClass), ex);
                }
            }
        } else {
            throw new IllegalStateException("GraphReader could not be determined");
        }
    }

    /**
     * Builds a {@link GraphWriter} instance to use. Attempts to detect the file format to be write using the file
     * extension or simply uses configurations provided by the user on the parameters given to the step.
     */
    private GraphWriter constructWriter() {
        final Object objectOrClass = parameters.get(IO.writer, this::detectFileType).get(0);
        if (objectOrClass instanceof GraphWriter)
            return (GraphWriter) objectOrClass;
        else if (objectOrClass instanceof String) {
            if (objectOrClass.equals(IO.graphson)) {
                final GraphSONMapper.Builder builder = GraphSONMapper.build();
                detectRegistries().forEach(builder::addRegistry);
                return GraphSONWriter.build().mapper(builder.create()).create();
            } else if (objectOrClass.equals(IO.gryo)){
                final GryoMapper.Builder builder = GryoMapper.build();
                detectRegistries().forEach(builder::addRegistry);
                return GryoWriter.build().mapper(builder.create()).create();
            } else if (objectOrClass.equals(IO.graphml))
                return GraphMLWriter.build().create();
            else {
                try {
                    final Class<?> graphWriterClazz = Class.forName((String) objectOrClass);
                    final Method build = graphWriterClazz.getMethod("build");
                    final GraphWriter.WriterBuilder builder = (GraphWriter.WriterBuilder) build.invoke(null);
                    return builder.create();
                } catch (Exception ex) {
                    throw new IllegalStateException(String.format("Could not construct the specified GraphWriter of %s", objectOrClass), ex);
                }
            }
        } else {
            throw new IllegalStateException("GraphWriter could not be determined");
        }
    }

    protected String detectFileType() {
        if (file.endsWith(".kryo"))
            return IO.gryo;
        else if (file.endsWith(".json"))
            return IO.graphson;
        else if (file.endsWith(".xml"))
            return IO.graphml;
        else
            throw new IllegalStateException("Could not detect the file format - specify the writer explicitly or rename file with a standard extension");
    }

    protected List<IoRegistry> detectRegistries() {
        final List<Object> k = parameters.get(IO.registry, null);
        return k.stream().map(cn -> {
            try {
                if (cn instanceof IoRegistry)
                    return (IoRegistry) cn;
                else {
                    final Class<?> clazz = Class.forName(cn.toString());
                    return (IoRegistry) clazz.getMethod("instance").invoke(null);
                }
            } catch (Exception ex) {
                throw new IllegalStateException(ex);
            }
        }).collect(Collectors.toList());
    }

    @Override
    public int hashCode() {
        final int hash = super.hashCode() ^ this.parameters.hashCode();
        return (null != this.file) ? (hash ^ file.hashCode()) : hash;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, file, this.parameters);
    }

    @Override
    public IoStep clone() {
        final IoStep clone = (IoStep) super.clone();
        clone.parameters = this.parameters.clone();
        clone.file = this.file;
        clone.mode = this.mode;
        return clone;
    }
}
