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

import org.apache.tinkerpop.gremlin.process.remote.traversal.strategy.decoration.RemoteStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectStep;
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
 * {@code sparql()} start step. That step adds a {@link InjectStep} to the traversal with the SPARQL query within
 * it as a string value. This strategy finds that step and compiles it to a Gremlin traversal which then replaces
 * the {@link InjectStep}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SparqlStrategy extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy>
        implements TraversalStrategy.DecorationStrategy {
    private static final SparqlStrategy INSTANCE = new SparqlStrategy();

    private static final Set<Class<? extends DecorationStrategy>> PRIORS = Collections.singleton(RemoteStrategy.class);

    private SparqlStrategy() {}

    public static SparqlStrategy instance() {
        return INSTANCE;
    }

    @Override
    public Set<Class<? extends DecorationStrategy>> applyPrior() {
        return PRIORS;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (!(traversal.getParent() instanceof EmptyStep))
            return;

        if (traversal.getSteps().size() == 1 && traversal.getEndStep() instanceof InjectStep) {
            final InjectStep stepWithSparql = (InjectStep) traversal.getEndStep();
            final Object[] injections = stepWithSparql.getInjections();
            if (injections.length == 1 && injections[0] instanceof String) {
                final String sparql = (String) injections[0];
                final Traversal<Vertex, ?> sparqlTraversal = SparqlToGremlinCompiler.compile(
                        traversal.getGraph().get(), sparql);
                TraversalHelper.removeAllSteps(traversal);
                sparqlTraversal.asAdmin().getSteps().forEach(s -> traversal.addStep(s));
            }
        }
    }
}
