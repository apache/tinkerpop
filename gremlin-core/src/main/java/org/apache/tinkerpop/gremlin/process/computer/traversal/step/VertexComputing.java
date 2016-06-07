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

package org.apache.tinkerpop.gremlin.process.computer.traversal.step;

import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface VertexComputing {

    /**
     * Set the {@link Computer} to be used to generate the {@link GraphComputer}.
     *
     * @param computer the computer specification.
     */
    public void setComputer(final Computer computer);

    /**
     * Get the {@link Computer} for generating the {@link GraphComputer}.
     * Inferences on the state of the {@link org.apache.tinkerpop.gremlin.process.traversal.Step}
     * within the {@link org.apache.tinkerpop.gremlin.process.traversal.Traversal} can be use applied here.
     *
     * @return the computer specification for generating the graph computer.
     */
    public Computer getComputer();

    /**
     * Generate the {@link VertexProgram}.
     *
     * @param graph  the {@link Graph} that the program will be executed over.
     * @param memory the {@link Memory} from the previous OLAP job if it exists, else its an empty memory structure.
     * @return the generated vertex program instance.
     */
    public VertexProgram generateProgram(final Graph graph, final Memory memory);

    /**
     * @deprecated As of release 3.2.1. Please use {@link VertexComputing#getComputer()}.
     */
    @Deprecated
    public default GraphComputer generateComputer(final Graph graph) {
        return this.getComputer().apply(graph);
    }
}
