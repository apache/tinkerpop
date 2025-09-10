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

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.GraphComputing;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public interface GraphStepContract<S, E extends Element> extends Step<S, E>, GraphComputing {

    /**
     * Concrete implementations of this contract that can be referenced as TinkerPop implementations.
     */
    List<Class<? extends Step>> CONCRETE_STEPS = Collections.unmodifiableList(Arrays.asList(GraphStep.class, GraphStepPlaceholder.class));

    Class<E> getReturnClass();

    boolean isStartStep();

    boolean returnsVertex();

    boolean returnsEdge();

    Object[] getIds();

    void clearIds();
}
