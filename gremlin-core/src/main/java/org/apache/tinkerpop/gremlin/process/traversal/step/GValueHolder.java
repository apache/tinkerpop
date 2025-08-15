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
package org.apache.tinkerpop.gremlin.process.traversal.step;

import org.apache.tinkerpop.gremlin.process.traversal.GValueManager;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Collection;

public interface GValueHolder<S, E> extends Step<S, E> {

    default void reduce() {
        // todo: maybe check to see if GValueManager was updated after traversal construction to spare this updateVariable
        final GValueManager manager = this.getTraversal().getGValueManager();
        manager.getGValues().forEach(gValue -> updateVariable(gValue.getName(), gValue.get()));
        Step<S, E> concreteStep = this.asConcreteStep();
        concreteStep.setId(this.getId());
        TraversalHelper.replaceStep(this, concreteStep, this.getTraversal());
    }

    Step<S, E> asConcreteStep();

    boolean isParameterized();

    void updateVariable(String name, Object value);

    Collection<GValue<?>> getGValues();
}
