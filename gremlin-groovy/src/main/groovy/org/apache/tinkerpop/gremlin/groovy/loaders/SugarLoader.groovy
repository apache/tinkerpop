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
package org.apache.tinkerpop.gremlin.groovy.loaders

import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.Traverser
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__
import org.apache.tinkerpop.gremlin.structure.*
import org.apache.tinkerpop.gremlin.structure.util.StringFactory

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class SugarLoader {

    private static final String NAME = "name";

    public static void load() {

        GremlinLoader.load();

        Traverser.metaClass.getProperty = { final String key ->
            TraverserCategory.get((Traverser) delegate, key);
        }
        // g.V.map{it.label()}
        Traverser.metaClass.methodMissing = { final String name, final def args ->
            ((Traverser) delegate).get()."$name"(*args);
        }

        // g.V.age
        GraphTraversal.metaClass.methodMissing = { final String name, final def args ->
            return ((GraphTraversal) delegate).values(name);
        }

        GraphTraversalSource.metaClass.getProperty = { final String key ->
            GraphTraversalSourceCategory.get((GraphTraversalSource) delegate, key);
        }

        // __.age and __.out
        __.metaClass.static.propertyMissing = { final String name ->
            return null != __.metaClass.getMetaMethod(name) ? __."$name"() : __.values(name);
        }
        // __.name
        __.metaClass.static.getName = {
            return __.values(NAME);
        }
        // out and age
        /*Object.metaClass.propertyMissing = { final String name ->
            if (name.equals(NAME))
                return __.values(NAME);
            else
                return __."$name";
        }*/

        Traverser.metaClass.mixin(TraverserCategory.class);
        GraphTraversalSource.metaClass.mixin(GraphTraversalSourceCategory.class);
        GraphTraversal.metaClass.mixin(GraphTraversalCategory.class);
        Vertex.metaClass.mixin(VertexCategory.class);
        Edge.metaClass.mixin(ElementCategory.class);
        VertexProperty.metaClass.mixin(ElementCategory.class);
    }

    public static class TraverserCategory {
        public static final get(final Traverser traverser, final String key) {
            return traverser.get()."$key";
        }

        public String toString() {
            return this.metaClass.owner.get().toString();
        }
    }

    public static class ElementCategory {
        public static final Object get(final Element element, final String key) {
            final Property property = element.property(key);
            if (property.isPresent())
                return property.value();
            else
                return element."$key"();
        }

        public static final set(final Element element, final String key, final Object value) {
            element.property(key, value);
        }

        public String toString() {
            if (this.metaClass.owner instanceof Vertex)
                return StringFactory.vertexString(this.metaClass.owner);
            else if (this.metaClass.owner instanceof Edge)
                return StringFactory.edgeString(this.metaClass.owner);
            else
                return StringFactory.propertyString(this.metaClass.owner);
        }
    }

    public static class VertexCategory {
        public static final Object get(final Vertex vertex, final String key) {
            final Property property = vertex.property(key);
            if (property.isPresent())
                return property.value();
            else
                return vertex."$key"();
        }

        public static final set(final Vertex vertex, final String key, final Object value) {
            vertex.property(VertexProperty.Cardinality.single, key, value);
        }

        public static final putAt(final Vertex vertex, final String key, final Object value) {
            vertex.property(VertexProperty.Cardinality.list, key, value);
        }

        public String toString() {
            return StringFactory.vertexString(this.metaClass.owner);
        }
    }

    public static class GraphTraversalSourceCategory {

        private static final String V = "V";
        private static final String E = "E";

        public static final get(final GraphTraversalSource graphTraversalSource, final String key) {
            if (key.equals(V))
                return graphTraversalSource.V();
            else if (key.equals(E))
                return graphTraversalSource.E();
            else
                throw new UnsupportedOperationException("The provided key does not reference a known method: " + key);
        }

        public String toString() {
            return StringFactory.traversalSourceString(this.metaClass.owner);
        }
    }

    public static class GraphTraversalCategory {

        public static final get(final GraphTraversal graphTraversal, final String key) {
            graphTraversal."$key"()
        }

        public static final getAt(final GraphTraversal graphTraversal, final Integer index) {
            graphTraversal.range(index, index + 1);
        }

        public static final getAt(final GraphTraversal graphTraversal, final Range range) {
            graphTraversal.range(range.getFrom() as Integer, range.getTo() as Integer);
        }

        public static final or(final GraphTraversal.Admin leftTraversal, final Traversal.Admin rightTraversal) {
            leftTraversal.or();
            rightTraversal.getSteps().forEach { step ->
                leftTraversal.addStep(step);
            }
            return leftTraversal;
        }

        public static final and(final GraphTraversal.Admin leftTraversal, final Traversal.Admin rightTraversal) {
            leftTraversal.and();
            rightTraversal.getSteps().forEach { step ->
                leftTraversal.addStep(step);
            }
            return leftTraversal;
        }

        public String toString() {
            return StringFactory.traversalString(this.metaClass.owner);
        }
    }
}