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
package org.apache.tinkerpop.gremlin.process.traversal.step.filter;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;

import java.util.Arrays;
import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class HasStepTest extends StepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.has("name"),
                __.has("name", "marko"),
                __.has("name", out("knows").values("name")),
                __.hasId(1),
                __.hasId(1.0),
                __.hasKey("name"),
                __.hasKey("age"),
                __.hasLabel("person"),
                __.hasLabel("project"),
                __.hasNot("name"),
                __.hasNot("age"),
                __.hasValue("marko"),
                __.hasValue("josh")
        );
    }
}
