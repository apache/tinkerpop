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

package org.apache.tinkerpop.gremlin.akka.process.actors;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.akka.process.actors.io.gryo.GryoSerializer;
import org.apache.tinkerpop.gremlin.process.actors.GraphActors;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalInterruptionTest;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.PartitionStrategyProcessTest;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.AbstractTinkerGraphProvider;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AkkaActorsProvider extends AbstractTinkerGraphProvider {

    private static final Random RANDOM = new Random();

    private static Set<String> SKIP_TESTS = new HashSet<>(Arrays.asList(
            "g_withBulkXfalseX_withSackX1_sumX_V_out_barrier_sack",
            "g_V_repeatXdedupX_timesX2X_count",
            "g_withSackXmap__map_cloneX_V_out_out_sackXmap_a_nameX_sack",
            "g_VX1X_sideEffectXstore_aX_name",
            "g_VX1X_out_sideEffectXincr_cX_name",
            SubgraphTest.Traversals.class.getCanonicalName(),
            ProfileTest.Traversals.class.getCanonicalName(),
            PartitionStrategyProcessTest.class.getCanonicalName(),
            EventStrategyProcessTest.class.getCanonicalName(),
            ElementIdStrategyProcessTest.class.getCanonicalName(),
            TraversalInterruptionTest.class.getCanonicalName()));

    @Override
    public Set<String> getSkipTests() {
        return SKIP_TESTS;
    }

    @Override
    public Map<String, Object> getBaseConfiguration(final String graphName, final Class<?> test, final String testMethodName,
                                                    final LoadGraphWith.GraphData loadGraphWith) {

        final Map<String, Object> configuration = super.getBaseConfiguration(graphName, test, testMethodName, loadGraphWith);
        // Akka specific configuration
        configuration.put(Constants.AKKA_LOG_DEAD_LETTERS_DURING_SHUTDOWN, false);
        configuration.put(Constants.AKKA_ACTOR_PROVIDER, "remote");
        configuration.put(Constants.AKKA_ACTOR_SERIALIZE_MESSAGES, "on");
        configuration.put(Constants.AKKA_ACTOR_SERIALIZERS_GRYO, GryoSerializer.class.getCanonicalName());
        configuration.put(Constants.AKKA_REMOTE_ENABLED_TRANSPORTS, Collections.singletonList("akka.remote.netty.tcp"));
        configuration.put(Constants.AKKA_REMOTE_NETTY_TCP_HOSTNAME, "127.0.0.1");
        configuration.put(Constants.AKKA_REMOTE_NETTY_TCP_PORT, 2552);
        return configuration;
    }

    @Override
    public GraphTraversalSource traversal(final Graph graph) {
        if (isSkipTest(graph.configuration()))
            return graph.traversal();
        else {
            final GraphTraversalSource g = graph.traversal();
            return RANDOM.nextBoolean() ?
                    g.withProcessor(AkkaGraphActors.open().workers(new Random().nextInt(15) + 1)) :
                    g.withProcessor(GraphActors.open(AkkaGraphActors.class));
        }
    }

    @Override
    public GraphActors getGraphActors(final Graph graph) {
        return AkkaGraphActors.open().workers(new Random().nextInt(15) + 1);
    }
}