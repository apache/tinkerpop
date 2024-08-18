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
package org.apache.tinkerpop.gremlin.process.traversal.step.util.structure.sideEffect;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.GValueStep;

import java.util.Arrays;

public class InjectStepGV<S> extends GValueStep<InjectStep, S, S> implements InjectStepStructure<GValue<S>> {

    private final GValue<S>[] injections;

    public InjectStepGV(final Traversal.Admin traversal, final GValue<S>... injections) {
        super(traversal, new InjectStep<>(traversal, Arrays.stream(injections).map(GValue::get).toArray(Object[]::new)));
        this.injections = injections;
    }

    public GValue<S>[] getInjections() {
        return injections;
    }
}
