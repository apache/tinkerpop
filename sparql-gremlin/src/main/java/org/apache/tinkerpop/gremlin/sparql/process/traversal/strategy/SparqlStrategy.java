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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.sparql.SparqlToGremlinCompiler;
import org.apache.tinkerpop.gremlin.sparql.process.traversal.dsl.sparql.SparqlTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collections;
import java.util.Set;

/**
 * This {@link TraversalStrategy} is used in conjunction with the {@link SparqlTraversalSource} which has a single
 * {@code sparql()} start step. That step adds a {@link InjectStep} to the traversal with the SPARQL query within
 * it as a string value. This strategy finds that step and compiles it to a Gremlin traversal which then replaces
 * the {@link InjectStep}.
 *
 * For remote bytecode execution, note that the {@code sparql-gremlin} dependencies need to be on the server. The
 * way remoting works - see {@code RemoteStep} - prevents modified bytecode from being submitted to the server, so
 * technically, this strategy can't be applied first on the client.
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
        if (!(traversal.isRoot()))
            return;

        // assumes that the traversal starts with the single inject step that holds the sparql query
        if (traversal.getStartStep() instanceof InjectStep) {
            final InjectStep stepWithSparql = (InjectStep) traversal.getStartStep();
            final Object[] injections = stepWithSparql.getInjections();

            // further assumes that there is just one argument to that injection which is a string (i.e. sparql query)
            if (injections.length == 1 && injections[0] instanceof String) {
                final String sparql = (String) injections[0];

                // try to grab the TraversalSource from the Traversal, but if it's not there then try to the Graph
                // instance and spawn one off from there.
                final Traversal<Vertex, ?> sparqlTraversal = SparqlToGremlinCompiler.compile(
                        (GraphTraversalSource) traversal.getTraversalSource().orElseGet(() -> traversal.getGraph().map(Graph::traversal).get()), sparql);
                TraversalHelper.insertTraversal(stepWithSparql, sparqlTraversal.asAdmin(), traversal);
                traversal.removeStep(stepWithSparql);
            }
        }
    }
}
