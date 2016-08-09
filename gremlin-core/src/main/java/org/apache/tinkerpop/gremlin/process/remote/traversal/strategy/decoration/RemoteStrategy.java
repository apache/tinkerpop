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
package org.apache.tinkerpop.gremlin.process.remote.traversal.strategy.decoration;

import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration.VertexProgramStrategy;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.remote.RemoteGraph;
import org.apache.tinkerpop.gremlin.process.remote.traversal.step.map.RemoteStep;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ProfileSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.VerificationException;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

import java.util.Collections;
import java.util.Set;

/**
 * Reconstructs a {@link Traversal} by appending a {@link RemoteStep} to its end. That step will submit the
 * {@link Traversal} to a {@link RemoteConnection} instance which will typically send it to a remote server for
 * execution and return results.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class RemoteStrategy extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy>
        implements TraversalStrategy.DecorationStrategy {

    private static final RemoteStrategy INSTANCE = new RemoteStrategy();

    private static final Set<Class<? extends DecorationStrategy>> POSTS = Collections.singleton(VertexProgramStrategy.class);

    private RemoteStrategy() {
    }

    public static RemoteStrategy instance() {
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

        // verifications to ensure unsupported steps do not exist in the traversal
        if (Boolean.valueOf(System.getProperty("is.testing", "false")) &&
                (TraversalHelper.hasStepOfAssignableClassRecursively(ProfileSideEffectStep.class, traversal) ||
                        traversal.getBytecode().toString().contains("$"))) {
            throw new VerificationException("RemoteGraph does not support profiling nor lambdas", traversal);
        }

        if (!(traversal.getGraph().orElse(EmptyGraph.instance()) instanceof RemoteGraph))
            throw new IllegalStateException("RemoteStrategy expects a RemoteGraph instance attached to the Traversal");

        final RemoteGraph remoteGraph = (RemoteGraph) traversal.getGraph().get();
        if (null == remoteGraph.getConnection())
            throw new IllegalStateException("RemoteStrategy expects the RemoteGraph instance to have a RemoteConnection");

        // remote step wraps the traversal and emits the results from the remote connection
        final RemoteStep<?, ?> remoteStep = new RemoteStep<>(traversal, remoteGraph.getConnection());
        TraversalHelper.removeAllSteps(traversal);
        traversal.addStep(remoteStep);

        // validations
        assert traversal.getStartStep().equals(remoteStep);
        assert traversal.getSteps().size() == 1;
        assert traversal.getEndStep() == remoteStep;
    }
}
