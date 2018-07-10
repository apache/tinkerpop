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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.step.Writing;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoWriter;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Writes data to a file from a {@link Graph}. This step is meant to be used as the first and last step in a
 * traversal.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class WriteStep extends AbstractStep<Map<String,Object>, Map<String,Object>> implements Writing {

    private Parameters parameters = new Parameters();
    private boolean first = true;
    private String file;

    public WriteStep(final Traversal.Admin traversal, final String file) {
        super(traversal);

        if (null == file || file.isEmpty())
            throw new IllegalArgumentException("file cannot be null or empty");

        this.file = file;
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
    protected Traverser.Admin<Map<String,Object>> processNextStart() {
        if (!this.first) throw FastNoSuchElementException.instance();

        this.first = false;
        final TraverserGenerator generator = this.getTraversal().getTraverserGenerator();

        final File file = new File(this.file);
        try (final OutputStream stream = new FileOutputStream(file)) {
            final Graph graph = (Graph) this.traversal.getGraph().get();
            GryoWriter.build().create().writeGraph(stream, graph);

            final Map<String, Object> stats = new LinkedHashMap<>();
            stats.put("vertices", IteratorUtils.count(graph.vertices()));
            stats.put("edges", IteratorUtils.count(graph.edges()));

            return generator.generate(stats, this, 1L);
        } catch (IOException ioe) {
            throw new IllegalStateException(String.format("Could not write file %s from graph", this.file), ioe);
        }
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
    public WriteStep clone() {
        final WriteStep clone = (WriteStep) super.clone();
        clone.parameters = this.parameters.clone();
        clone.file = this.file;
        return clone;
    }
}
