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
package org.apache.tinkerpop.gremlin.server.util;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.AbstractTinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;

import java.util.Map;

/**
 * A {@link LifeCycleHook} that loads TinkerFactory sample datasets into a graph during server startup.
 * Replaces the Groovy init scripts (e.g. generate-modern.groovy) for default server configurations.
 *
 * <p>Configuration parameters (via {@code config} in YAML):
 * <ul>
 *   <li>{@code graph} — name of the graph (as defined in {@code graphs:} config) to load data into</li>
 *   <li>{@code dataset} — which dataset to load: modern, classic, crew, grateful, sink, airroutes</li>
 * </ul>
 *
 * <p>Example YAML usage:
 * <pre>
 * lifecycleHooks:
 *   - className: org.apache.tinkerpop.gremlin.server.util.TinkerFactoryDataLoader
 *     config: {graph: graph, dataset: modern}
 * </pre>
 */
public class TinkerFactoryDataLoader implements LifeCycleHook {

    private String graphName;
    private String dataset;

    @Override
    public void init(final Map<String, Object> config) {
        graphName = (String) config.get("graph");
        dataset = (String) config.get("dataset");
        if (graphName == null || dataset == null) {
            throw new IllegalArgumentException("TinkerFactoryDataLoader requires 'graph' and 'dataset' config parameters");
        }
    }

    @Override
    public void onStartUp(final Context c) {
        final Graph graph = c.getGraphManager().getGraph(graphName);
        if (null == graph) {
            c.getLogger().warn("TinkerFactoryDataLoader could not find graph [{}]", graphName);
            return;
        }
        if (!(graph instanceof AbstractTinkerGraph)) {
            c.getLogger().warn("TinkerFactoryDataLoader requires an AbstractTinkerGraph but [{}] is {}",
                               graphName, graph.getClass().getName());
            return;
        }

        final AbstractTinkerGraph tinkerGraph = (AbstractTinkerGraph) graph;
        switch (dataset) {
            case "modern":
                TinkerFactory.generateModern(tinkerGraph);
                break;
            case "classic":
                TinkerFactory.generateClassic(tinkerGraph);
                break;
            case "crew":
                TinkerFactory.generateTheCrew(tinkerGraph);
                break;
            case "grateful":
                TinkerFactory.generateGratefulDead(tinkerGraph);
                break;
            case "sink":
                TinkerFactory.generateKitchenSink(tinkerGraph);
                break;
            case "airroutes":
                TinkerFactory.generateAirRoutes(tinkerGraph);
                break;
            default:
                c.getLogger().warn("TinkerFactoryDataLoader unknown dataset [{}]", dataset);
                return;
        }
        c.getLogger().info("TinkerFactoryDataLoader loaded [{}] dataset into graph [{}]", dataset, graphName);
    }

    @Override
    public void onShutDown(final Context c) {
        // nothing to do on shutdown
    }
}
