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
package org.apache.tinkerpop.gremlin.structure.strategy.process.graph.step.sideEffect;

import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.graph.traversal.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.strategy.StrategyEdge;
import org.apache.tinkerpop.gremlin.structure.strategy.StrategyGraph;
import org.apache.tinkerpop.gremlin.structure.strategy.StrategyVertex;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StrategyGraphStep<E extends Element> extends GraphStep<E> {

    private final GraphTraversal<?, E> graphTraversal;

    public StrategyGraphStep(final Traversal.Admin traversal, final StrategyGraph strategyGraph, final Class<E> returnClass, final GraphTraversal<?, E> graphTraversal) {
        super(traversal, strategyGraph, returnClass);
        this.graphTraversal = graphTraversal;
        this.setIteratorSupplier(() -> (Iterator) (Vertex.class.isAssignableFrom(this.returnClass) ?
                new StrategyVertex.StrategyVertexIterator((Iterator) this.graphTraversal, strategyGraph) :
                new StrategyEdge.StrategyEdgeIterator((Iterator) this.graphTraversal, strategyGraph)));
    }
}
