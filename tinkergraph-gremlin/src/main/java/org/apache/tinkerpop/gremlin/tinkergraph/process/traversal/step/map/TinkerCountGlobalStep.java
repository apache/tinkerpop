/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerHelper;

import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TinkerCountGlobalStep<S extends Element> extends AbstractStep<S, Long> {

    private final Class<S> elementClass;
    private boolean done = false;

    public TinkerCountGlobalStep(final Traversal.Admin traversal, final Class<S> elementClass) {
        super(traversal);
        this.elementClass = elementClass;
    }

    @Override
    protected Traverser.Admin<Long> processNextStart() throws NoSuchElementException {
        if (!this.done) {
            this.done = true;
            final TinkerGraph graph = (TinkerGraph) this.getTraversal().getGraph().get();
            return this.getTraversal().getTraverserGenerator().generate(Vertex.class.isAssignableFrom(this.elementClass) ?
                            (long) TinkerHelper.getVertices(graph).size() :
                            (long) TinkerHelper.getEdges(graph).size(),
                    (Step) this, 1L);
        } else
            throw FastNoSuchElementException.instance();
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.elementClass.getSimpleName().toLowerCase());
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.elementClass.hashCode();
    }

    @Override
    public void reset() {
        this.done = false;
    }
}
