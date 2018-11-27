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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions;

import java.util.Arrays;
import java.util.List;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class PropertyMapStepTest extends StepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.valueMap(),
                __.valueMap("name"),
                __.valueMap("age"),
                __.valueMap("name", "age"),
                __.valueMap().with(WithOptions.tokens),
                __.valueMap("name").with(WithOptions.tokens),
                __.valueMap("age").with(WithOptions.tokens),
                __.valueMap("name", "age").with(WithOptions.tokens),
                __.valueMap("name", "age").with(WithOptions.tokens, WithOptions.ids),
                __.valueMap("name", "age").with(WithOptions.tokens, WithOptions.labels),
                __.propertyMap(),
                __.propertyMap("name"),
                __.propertyMap("age"),
                __.propertyMap("name", "age")
        );
    }
}
