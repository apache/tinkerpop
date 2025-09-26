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

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.step.Configuring;
import org.apache.tinkerpop.gremlin.process.traversal.step.Deleting;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.Writing;
import org.apache.tinkerpop.gremlin.process.traversal.step.PropertiesHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.HashSet;

public interface AddPropertyStepContract<S> extends Step<S, S>, TraversalParent, Scoping, PropertiesHolder,
        Writing<Event.ElementPropertyChangedEvent>, Deleting<Event.ElementPropertyChangedEvent>, Configuring {
    VertexProperty.Cardinality getCardinality();

    @Override
    default HashSet<PopInstruction> getPopInstructions() {
        final HashSet<PopInstruction> popInstructions = new HashSet<>();
        popInstructions.addAll(Scoping.super.getPopInstructions());
        popInstructions.addAll(TraversalParent.super.getPopInstructions());
        return popInstructions;
    }

    /**
     * Get the property key
     */
    Object getKey();

    /**
     * Gets the property value. If the value was originally passed as a {@link GValue<?>}, the variable will become pinned
     * and the literal value returned.
     */
    Object getValue();

    /**
     * Gets the property value. If the value was originally passed as a {@link GValue<?>} it is returned as-is, without
     * pinning the variable.
     */
    default Object getValueWithGValue() {
        return getValue();
    }
}
