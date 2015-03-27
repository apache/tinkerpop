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
package org.apache.tinkerpop.gremlin.process.traversal.step.util.event;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class EdgePropertyChangedEvent extends ElementPropertyChangedEvent {

    public EdgePropertyChangedEvent(final Edge edge, final Property oldValue, final Object newValue) {
        super(edge, oldValue, newValue);
    }

    @Override
    void fire(final MutationListener listener, final Element element, final Property oldValue, final Object newValue, final Object... vertexPropertyKeyValues) {
        listener.edgePropertyChanged((Edge) element, oldValue, newValue);
    }
}
