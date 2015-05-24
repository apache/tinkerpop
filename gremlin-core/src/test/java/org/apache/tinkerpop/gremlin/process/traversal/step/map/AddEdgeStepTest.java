/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class AddEdgeStepTest extends StepTest {

    @Override
    protected List<Traversal> getTraversals() {
        final Vertex v1 = new DetachedVertex(1, Vertex.DEFAULT_LABEL, new HashMap<>());
        final Vertex v2 = new DetachedVertex(2, Vertex.DEFAULT_LABEL, new HashMap<>());
        final List<Vertex> v = Arrays.asList(v1, v2);
        return Arrays.asList(
                __.addE(Direction.IN, "knows", v1),
                __.addE(Direction.IN, "knows", v2),
                __.addE(Direction.IN, "knows", v.iterator()),
                __.addE(Direction.OUT, "knows", v1),
                __.addE(Direction.IN, "knows", v1, "weight", 0),
                __.addE(Direction.IN, "knows", v1, "weight", 1),
                __.addE(Direction.IN, "knows", v.iterator(), "weight", 0)
        );
    }
}
