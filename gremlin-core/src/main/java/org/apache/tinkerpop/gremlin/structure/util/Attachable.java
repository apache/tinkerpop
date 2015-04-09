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
package org.apache.tinkerpop.gremlin.structure.util;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

/**
 * An interface that provides methods for detached properties and elements to be re-attached to the {@link Graph}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface Attachable<T> {
    public abstract T attach(final Vertex hostVertex) throws IllegalStateException;

    public abstract T attach(final Graph hostGraph) throws IllegalStateException;

    public static class Exceptions {

        private Exceptions() {
        }

        public static IllegalStateException canNotAttachVertexToHostVertex(final Attachable<Vertex> vertex, final Vertex hostVertex) {
            return new IllegalStateException("The provided vertex is not the host vertex: " + vertex + " does not equal " + hostVertex);
        }

        public static IllegalStateException canNotAttachVertexToHostGraph(final Attachable<Vertex> vertex, final Graph hostGraph) {
            return new IllegalStateException("The provided vertex could not be found in the host graph: " + vertex + " not in " + hostGraph);
        }

        public static IllegalStateException canNotAttachEdgeToHostVertex(final Attachable<Edge> edge, final Vertex hostVertex) {
            return new IllegalStateException("The provided edge is not incident to the host vertex: " + edge + " not incident to " + hostVertex);
        }

        public static IllegalStateException canNotAttachEdgeToHostGraph(final Attachable<Edge> edge, final Graph hostGraph) {
            return new IllegalStateException("The provided edge could not be found in the host graph: " + edge + " not in " + hostGraph);
        }

        public static IllegalStateException canNotAttachVertexPropertyToHostVertex(final Attachable<VertexProperty> vertexProperty, final Vertex hostVertex) {
            return new IllegalStateException("The provided vertex property is not a property of the host vertex: " + vertexProperty + " not a property of " + hostVertex);
        }

        public static IllegalStateException canNotAttachVertexPropertyToHostGraph(final Attachable<VertexProperty> vertexProperty, final Graph hostGraph) {
            return new IllegalStateException("The provided vertex property could not be found in the host graph: " + vertexProperty + " not in " + hostGraph);
        }

        public static IllegalStateException canNotAttachPropertyToHostVertex(final Attachable<Property> property, final Vertex hostVertex) {
            return new IllegalStateException("The provided property could not be attached the host vertex: " + property + " not a property in the star of " + hostVertex);
        }

        public static IllegalStateException canNotAttachPropertyToHostGraph(final Attachable<Property> property, final Graph hostGraph) {
            return new IllegalStateException("The provided property could not be attached the host graph: " + property + " not in " + hostGraph);
        }
    }


}