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
import org.apache.tinkerpop.gremlin.process.traversal.step.Configuring;
import org.apache.tinkerpop.gremlin.process.traversal.step.Reading;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Reads data from a file into a {@link Graph}. This step is meant to be used as the first and last step in a
 * traversal.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ReadStep extends AbstractStep<Map<String,Object>, Map<String,Object>> implements Reading {

    private Parameters parameters = new Parameters();
    private boolean first = true;
    private String localFile;

    public ReadStep(final Traversal.Admin traversal, final String localFile) {
        super(traversal);

        if (null == localFile || localFile.isEmpty())
            throw new IllegalArgumentException("localFile cannot be null or empty");

        this.localFile = localFile;
    }

    @Override
    public String getLocalFile() {
        return localFile;
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
        final File file = new File(localFile);
        if (!file.exists()) throw new IllegalStateException(localFile + " does not exist");

        try (final InputStream stream = new FileInputStream(file)) {
            final Graph graph = (Graph) this.traversal.getGraph().get();
            GryoReader.build().create().readGraph(stream, graph);

            final Map<String,Object> stats = new LinkedHashMap<>();
            stats.put("vertices", IteratorUtils.count(graph.vertices()));
            stats.put("edges", IteratorUtils.count(graph.edges()));

            return generator.generate(stats, this, 1L);
        } catch (IOException ioe) {
            throw new IllegalStateException(String.format("Could not read file %s into graph", localFile), ioe);
        }
    }

    @Override
    public int hashCode() {
        final int hash = super.hashCode() ^ this.parameters.hashCode();
        return (null != this.localFile) ? (hash ^ localFile.hashCode()) : hash;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, localFile, this.parameters);
    }

    @Override
    public ReadStep clone() {
        final ReadStep clone = (ReadStep) super.clone();
        clone.parameters = this.parameters.clone();
        clone.localFile = this.localFile;
        return clone;
    }
}
