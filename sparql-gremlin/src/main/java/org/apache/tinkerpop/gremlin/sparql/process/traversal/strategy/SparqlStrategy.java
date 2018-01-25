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
package org.apache.tinkerpop.gremlin.sparql.process.traversal.strategy;

import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration.VertexProgramStrategy;
import org.apache.tinkerpop.gremlin.process.remote.traversal.strategy.decoration.RemoteStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ConstantStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.sparql.SparqlToGremlinCompiler;
import org.apache.tinkerpop.gremlin.sparql.process.traversal.dsl.sparql.SparqlTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collections;
import java.util.Set;

/**
 * This {@link TraversalStrategy} is used in conjunction with the {@link SparqlTraversalSource} which has a single
 * {@code sparql()} start step. That step adds a {@link ConstantStep} to the traversal with the SPARQL query within
 * it as a string value. This strategy finds that step and transpiles it to a Gremlin traversal which then replaces
 * the {@link ConstantStep}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SparqlStrategy extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy>
        implements TraversalStrategy.DecorationStrategy {
    private static final SparqlStrategy INSTANCE = new SparqlStrategy();

    private static final Set<Class<? extends DecorationStrategy>> POSTS = Collections.singleton(RemoteStrategy.class);

    private SparqlStrategy() {}

    public static SparqlStrategy instance() {
        return INSTANCE;
    }

    @Override
    public Set<Class<? extends DecorationStrategy>> applyPost() {
        return POSTS;
    }


    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (!(traversal.getParent() instanceof EmptyStep))
            return;

        if (traversal.getSteps().size() == 2 && traversal.getEndStep() instanceof ConstantStep) {
            final ConstantStep stepWithSparql = (ConstantStep) traversal.getEndStep();
            final Object constant = stepWithSparql.getConstant();
            if (constant instanceof String) {
                final String sparql = (String) constant;
                final Traversal<Vertex, ?> sparqlTraversal = SparqlToGremlinCompiler.convertToGremlinTraversal(
                        traversal.getGraph().get(), sparql);
                TraversalHelper.removeAllSteps(traversal);
                sparqlTraversal.asAdmin().getSteps().forEach(s -> traversal.addStep(s));
            } else {
                // The ConstantStep expects a string value
                throw new IllegalStateException("SparqlStrategy cannot be applied to a traversal that does not consist of a single ConstantStep<?,String>");
            }
        } else {
            // SparqlStrategy requires that there be one step and it be a ConstantStep that contains some SPARQL
            throw new IllegalStateException("SparqlStrategy cannot be applied to a traversal that does not consist of a single ConstantStep<?,String>");
        }
    }
}
