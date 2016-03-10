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
package org.apache.tinkerpop.gremlin.process.server.traversal.strategy;

import org.apache.tinkerpop.gremlin.process.server.ServerConnection;
import org.apache.tinkerpop.gremlin.process.server.ServerGraph;
import org.apache.tinkerpop.gremlin.process.server.traversal.step.map.ServerResultStep;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.VertexProgramStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.StandardVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Collections;
import java.util.Set;

/**
 * Reconstructs a {@link Traversal} by appending a {@link ServerResultStep} to its end. That step will submit the
 * {@link Traversal} to a {@link ServerConnection} instance which will typically send it to a remote server for
 * execution and return results.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ServerStrategy extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy>
        implements TraversalStrategy.DecorationStrategy {

    private static final ServerStrategy INSTANCE = new ServerStrategy();

    private ServerStrategy() {}

    public static ServerStrategy instance() {
        return INSTANCE;
    }

    @Override
    public Set<Class<? extends DecorationStrategy>> applyPost() {
        return Collections.singleton(VertexProgramStrategy.class);
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (!(traversal.getParent() instanceof EmptyStep))
            return;

        if (!traversal.getGraph().isPresent())
            throw new IllegalStateException("ServerStrategy expects a ServerGraph instance attached to the Traversal");

        if (!(traversal.getGraph().get() instanceof ServerGraph))
            throw new IllegalStateException("ServerStrategy expects a ServerGraph instance attached to the Traversal");

        final ServerGraph serverGraph = (ServerGraph) traversal.getGraph().get();
        if (null == serverGraph.getConnection())
            throw new IllegalStateException("ServerStrategy expects ServerGraph instance to have a ServerConnection");

        final Traversal.Admin<?, ?> serverTraversal = new DefaultTraversal<>();
        TraversalHelper.removeToTraversal(traversal.getStartStep(), EmptyStep.instance(), (Traversal.Admin) serverTraversal);
        final ServerResultStep serverStep = new ServerResultStep(traversal, serverTraversal, serverGraph.getConnection());
        traversal.addStep(serverStep);

        assert traversal.getStartStep().equals(serverStep);
        assert traversal.getSteps().size() == 1;
        assert traversal.getEndStep() == serverStep;
    }
}
