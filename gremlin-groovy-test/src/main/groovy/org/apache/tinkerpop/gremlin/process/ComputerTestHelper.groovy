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
package org.apache.tinkerpop.gremlin.process

import org.apache.tinkerpop.gremlin.process.computer.ComputerResult
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram
import org.apache.tinkerpop.gremlin.process.graph.traversal.GraphTraversalContext
import org.apache.tinkerpop.gremlin.process.traversal.engine.ComputerTraversalEngine
import org.apache.tinkerpop.gremlin.structure.Graph

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ComputerTestHelper {

    public static final Traversal compute(
            final Graph graph,
            final TraversalContext.Builder builder,
            final String scriptEngineName,
            final String traversalScript) {

        final TraversalVertexProgram program = TraversalVertexProgram.build().traversal(graph.getClass(), builder, scriptEngineName, traversalScript).create();
        final ComputerResult result = ((ComputerTraversalEngine) builder.getTraversalEngineBuilder().create(graph)).getGraphComputer().program(program).submit().get();
        return program.computerResultTraversal(result);
    }

    public static final Traversal compute(final String script, final GraphTraversalContext g) {
        return ComputerTestHelper.compute(g.getGraph(), g.asBuilder(), "gremlin-groovy", script);
    }
}
