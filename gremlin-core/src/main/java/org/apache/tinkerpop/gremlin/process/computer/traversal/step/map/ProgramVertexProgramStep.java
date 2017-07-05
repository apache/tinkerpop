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
import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ProgramVertexProgramStep extends VertexProgramStep {

    private final Map<String, Object> configuration;
    private final String toStringOfVertexProgram;
    private final Set<TraverserRequirement> traverserRequirements;

    public ProgramVertexProgramStep(final Traversal.Admin traversal, final VertexProgram vertexProgram) {
        super(traversal);
        this.configuration = new HashMap<>();
        final MapConfiguration base = new MapConfiguration(this.configuration);
        base.setDelimiterParsingDisabled(true);
        vertexProgram.storeState(base);
        this.toStringOfVertexProgram = vertexProgram.toString();
        this.traverserRequirements = vertexProgram.getTraverserRequirements();
    }

    @Override
    public VertexProgram generateProgram(final Graph graph, final Memory memory) {
        final MapConfiguration base = new MapConfiguration(this.configuration);
        base.setDelimiterParsingDisabled(true);
        PureTraversal.storeState(base, ROOT_TRAVERSAL, TraversalHelper.getRootTraversal(this.getTraversal()).clone());
        base.setProperty(STEP_ID, this.getId());
        if (memory.exists(TraversalVertexProgram.HALTED_TRAVERSERS))
            TraversalVertexProgram.storeHaltedTraversers(base, memory.get(TraversalVertexProgram.HALTED_TRAVERSERS));
        return VertexProgram.createVertexProgram(graph, base);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.traverserRequirements;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.configuration.hashCode();
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.toStringOfVertexProgram, new GraphFilter(this.computer));
    }
}
