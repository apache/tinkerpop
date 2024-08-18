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
package org.apache.tinkerpop.gremlin.process.traversal.step.util.structure.map;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.GValueStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;

import java.util.Arrays;

public class GraphStepGV<S, E> extends GValueStep<GraphStep, E, E> implements GraphStepStructure<E, GValue<Object>> {
    private final Parameters parameters = new Parameters();
    private final GValue<Object>[] ids;
    private final Class<E> returnClass;

    public GraphStepGV(final Traversal.Admin traversal, final Class<E> returnClass, final boolean isStart, final GValue<Object>... ids) {
        super(traversal, new GraphStep(traversal, returnClass, isStart,
                Arrays.stream(ids).map(GValue::get).toArray(Object[]::new)));
        this.returnClass = returnClass;
        this.ids = ids;
    }

    @Override
    public void configure(final Object... keyValues) {
        // all GValues locally stored in the parameters, but resolve them for the concrete step
        this.parameters.set(null, GValue.promoteGValuesInKeyValues(keyValues));
        this.concreteStep.configure(GValue.resolveGValues(keyValues));
    }

    @Override
    public Parameters getParameters() {
        return concreteStep.getParameters();
    }

    @Override
    public GValue<Object>[] getIds() {
        return ids;
    }

    @Override
    public Class<E> getReturnClass() {
        return returnClass;
    }
}
