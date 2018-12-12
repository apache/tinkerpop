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

package org.apache.tinkerpop.gremlin.groovy.jsr223;

import org.apache.tinkerpop.gremlin.GraphProvider;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.structure.RemoteGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphComputer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@GraphProvider.Descriptor(computer = TinkerGraphComputer.class)
public class GryoRemoteGraphGroovyTranslatorComputerProvider extends GryoRemoteGraphGroovyTranslatorProvider {

    @Override
    public GraphTraversalSource traversal(final Graph graph) {
        assert graph instanceof RemoteGraph;
        final int state = TestHelper.RANDOM.nextInt(3);
        switch (state) {
            case 0:
                return super.traversal(graph).withComputer();
            case 1:
                return super.traversal(graph).withComputer(Computer.compute(TinkerGraphComputer.class));
            case 2:
                return super.traversal(graph).withComputer(Computer.compute(TinkerGraphComputer.class).workers(1));
            default:
                throw new IllegalStateException("This state should not have occurred: " + state);
        }
    }
}
