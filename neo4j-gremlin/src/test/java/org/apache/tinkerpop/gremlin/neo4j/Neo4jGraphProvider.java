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
package org.apache.tinkerpop.gremlin.neo4j;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.OrderabilityTest",
        method = "g_V_properties_order",
        reason = "Cannot order by vertex property id")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.OrderabilityTest",
        method = "g_V_out_out_properties_asXheadX_path_order_byXascX_selectXheadX_value",
        reason = "Cannot order by vertex property id")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.OrderabilityTest",
        method = "g_V_out_out_properties_asXheadX_path_order_byXdescX_selectXheadX_value",
        reason = "Cannot order by vertex property id")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.OrderabilityTest",
        method = "g_V_out_outE_order_byXascX",
        reason = "Cannot order by edge id")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.OrderabilityTest",
        method = "g_V_out_outE_asXheadX_path_order_byXascX_selectXheadX",
        reason = "Cannot order by edge id")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.OrderabilityTest",
        method = "g_V_out_outE_order_byXdescX",
        reason = "Cannot order by edge id")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.OrderabilityTest",
        method = "g_V_out_outE_asXheadX_path_order_byXdescX_selectXheadX",
        reason = "Cannot order by edge id")
public class Neo4jGraphProvider extends AbstractNeo4jGraphProvider {
    @Override
    public Map<String, Object> getBaseConfiguration(final String graphName, final Class<?> test, final String testMethodName, final LoadGraphWith.GraphData graphData) {
        final String directory = makeTestDirectory(graphName, test, testMethodName);

        return new HashMap<String, Object>() {{
            put(Graph.GRAPH, Neo4jGraph.class.getName());
            put(Neo4jGraph.CONFIG_DIRECTORY, directory);
            put(Neo4jGraph.CONFIG_CONF + ".dbms.memory.pagecache.size", "1m");
        }};
    }
}
