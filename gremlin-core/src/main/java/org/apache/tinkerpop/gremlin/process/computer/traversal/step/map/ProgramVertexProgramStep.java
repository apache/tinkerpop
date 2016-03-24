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

package org.apache.tinkerpop.gremlin.process.computer.traversal.step.map;

import org.apache.commons.configuration.MapConfiguration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Collections;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ProgramVertexProgramStep extends VertexProgramStep {

    final Map<String, Object> configuration;

    public ProgramVertexProgramStep(final Traversal.Admin traversal, final VertexProgram vertexProgram) {
        super(traversal);
        final MapConfiguration base = new MapConfiguration(Collections.emptyMap());
        base.setDelimiterParsingDisabled(true);
        vertexProgram.storeState(base);
        this.configuration = base.getMap();
    }

    @Override
    public VertexProgram generateProgram(final Graph graph) {
        return VertexProgram.createVertexProgram(graph, new MapConfiguration(this.configuration));
    }

    @Override
    public GraphComputer generateComputer(final Graph graph) {
        return this.computer.apply(graph);
    }
}
