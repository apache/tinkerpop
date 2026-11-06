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
package org.apache.tinkerpop.gremlin.process.traversal.dsl;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStartStep;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

import java.util.List;
import java.util.Set;

/**
 * Exercises the generated multi-label {@code addV} overloads on a {@link GremlinDsl}-generated
 * {@code TraversalSource} to verify that {@code additionalLabels} are actually threaded through to the
 * underlying {@link AddVertexStartStep} rather than being dropped.
 */
public class AddVMultiLabelDslTraversal {

    /**
     * Exposed for inspection by the test so it can assert on the label(s) applied by the underlying
     * {@link AddVertexStartStep} for {@code addV(String, String...)}.
     */
    public final Object stringLabelResult;

    /**
     * Exposed for inspection by the test so it can assert on the label traversal children applied by the
     * underlying {@link AddVertexStartStep} for {@code addV(Traversal, Traversal...)}.
     */
    public final List<Traversal.Admin<?, ?>> traversalLabelChildren;

    public AddVMultiLabelDslTraversal() {
        final SocialTraversalSource g = new SocialTraversalSource(EmptyGraph.instance());

        final GraphTraversal<?, ?> stringLabelTraversal = g.addV("person", "employee");
        final AddVertexStartStep stringLabelStep =
                (AddVertexStartStep) stringLabelTraversal.asAdmin().getStartStep();
        this.stringLabelResult = stringLabelStep.getLabel();

        final GraphTraversal<?, ?> traversalLabelTraversal = g.addV(__.constant("person"), __.constant("employee"));
        final AddVertexStartStep traversalLabelStep =
                (AddVertexStartStep) traversalLabelTraversal.asAdmin().getStartStep();
        this.traversalLabelChildren = (List<Traversal.Admin<?, ?>>) (List) traversalLabelStep.getLocalChildren();
    }

    public Object getStringLabelResult() {
        return stringLabelResult;
    }

    public List<Traversal.Admin<?, ?>> getTraversalLabelChildren() {
        return traversalLabelChildren;
    }
}
