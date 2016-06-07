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

package org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.Arrays;
import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.count;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@Deprecated
public class GroupSideEffectStepV3d0Test extends StepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.groupV3d0("a").by(T.label),
                __.groupV3d0("a").by(T.label).by("name"),
                __.groupV3d0("a").by(T.label).by("name").by(count(Scope.local))
        );
    }
}
