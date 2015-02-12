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
package org.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.EngineDependent;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphStep<S extends Element> extends StartStep<S> implements EngineDependent {

    protected final Class<S> returnClass;
    protected final Object[] ids;
    protected transient Graph graph;
    protected transient Supplier<Iterator<S>> iteratorSupplier;

    public GraphStep(final Traversal.Admin traversal, final Graph graph, final Class<S> returnClass, final Object... ids) {
        super(traversal);
        this.graph = graph;
        this.returnClass = returnClass;
        this.ids = ids;
        this.iteratorSupplier = () -> (Iterator<S>) (Vertex.class.isAssignableFrom(this.returnClass) ?
                this.graph.iterators().<S>vertexIterator(this.ids) :
                this.graph.iterators().edgeIterator(this.ids));
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, Arrays.asList(this.ids), this.returnClass.getSimpleName().toLowerCase());
    }

    public boolean returnsVertices() {
        return Vertex.class.isAssignableFrom(this.returnClass);
    }

    public boolean returnsEdges() {
        return Edge.class.isAssignableFrom(this.returnClass);
    }

    public Class<S> getReturnClass() {
        return this.returnClass;
    }

    public void setIteratorSupplier(final Supplier<Iterator<S>> iteratorSupplier) {
        this.iteratorSupplier = iteratorSupplier;
    }

    public <G extends Graph> G getGraph(final Class<G> graphClass) {
        return (G) this.graph;
    }

    public Object[] getIds() {
        return this.ids;
    }

    @Override
    public void onEngine(final TraversalEngine traversalEngine) {
        if (traversalEngine.equals(TraversalEngine.COMPUTER)) {
            this.iteratorSupplier = Collections::emptyIterator;
        }
    }

    @Override
    protected Traverser<S> processNextStart() {
        if (this.first)
            this.start = null == this.iteratorSupplier ? EmptyIterator.instance() : this.iteratorSupplier.get();
        return super.processNextStart();
    }
}
