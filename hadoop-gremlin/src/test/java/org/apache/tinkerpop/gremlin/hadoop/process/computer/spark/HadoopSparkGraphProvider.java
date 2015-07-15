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
package org.apache.tinkerpop.gremlin.hadoop.process.computer.spark;

import org.apache.tinkerpop.gremlin.GraphProvider;
import org.apache.tinkerpop.gremlin.hadoop.HadoopGraphProvider;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.giraph.GiraphGraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.engine.ComputerTraversalEngine;
import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@GraphProvider.Descriptor(computer = SparkGraphComputer.class)
public final class HadoopSparkGraphProvider extends HadoopGraphProvider {

    public GraphTraversalSource traversal(final Graph graph) {
        return GraphTraversalSource.build().engine(ComputerTraversalEngine.build().computer(SparkGraphComputer.class)).create(graph);
    }
}
