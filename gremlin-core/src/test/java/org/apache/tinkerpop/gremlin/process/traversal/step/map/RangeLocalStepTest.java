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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueStepTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class RangeLocalStepTest extends GValueStepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.limit(Scope.local, 10L),
                __.skip(Scope.local, 9L), // TODO:: best to edit this to __.skip(Scope.local, 10L) following resolution of TINKERPOP-3170
                __.range(Scope.local, 1L, 10L),
                __.limit(Scope.local, GValue.of("limit", 10L)),
                __.skip(Scope.local, GValue.of("skip", 9L)),
                __.range(Scope.local, GValue.of("low", 1L), GValue.of("high", 10L))
        );
    }

    @Override
    protected List<Pair<Traversal, Set<String>>> getGValueTraversals() {
        return List.of(
                Pair.of(__.limit(Scope.local, GValue.of("limit", 10L)), Set.of("limit")),
                Pair.of(__.skip(Scope.local, GValue.of("skip", 10L)), Set.of("skip")),
                Pair.of(__.range(Scope.local, GValue.of("low", 1L), GValue.of("high", 10L)), Set.of("low", "high"))
        );
    }
}
