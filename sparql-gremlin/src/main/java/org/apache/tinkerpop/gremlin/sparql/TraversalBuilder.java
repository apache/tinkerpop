/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tinkerpop.gremlin.sparql;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.PropertyType;
import org.apache.tinkerpop.gremlin.structure.Vertex;


class TraversalBuilder {

    public static GraphTraversal<?, ?> transform(final Triple triple) {
        final GraphTraversal<Vertex, ?> matchTraversal = __.as(triple.getSubject().getName());
        
        final Node predicate = triple.getPredicate();
        final String uri = predicate.getURI();
        final String uriValue = Prefixes.getURIValue(uri);
        final String prefix = Prefixes.getPrefix(uri);

        switch (prefix) {
            case "edge":
                return matchTraversal.out(uriValue).as(triple.getObject().getName());
            case "property":
                return matchProperty(matchTraversal, uriValue, PropertyType.PROPERTY, triple.getObject());
            case "value":
                return matchProperty(matchTraversal, uriValue, PropertyType.VALUE, triple.getObject());
            default:
                throw new IllegalStateException(String.format("Unexpected predicate: %s", predicate));
        }
    }

    private static GraphTraversal<?, ?> matchProperty(final GraphTraversal<?, ?> traversal, final String propertyName,
                                                      final PropertyType type, final Node object) {
        switch (propertyName) {
            case "id":
         
                return object.isConcrete()
                        ? traversal.hasId(object.getLiteralValue())
                        : traversal.id().as(object.getName());
            case "label":
                return object.isConcrete()
                        ? traversal.hasLabel(object.getLiteralValue().toString())
                        : traversal.label().as(object.getName());
            case "key":
                return object.isConcrete()
                        ? traversal.hasKey(object.getLiteralValue().toString())
                        : traversal.key().as(object.getName());
            case "value":
                return object.isConcrete()
                        ? traversal.hasValue(object.getLiteralValue().toString())
                        : traversal.value().as(object.getName());
            default:
                if (type.equals(PropertyType.PROPERTY)) {
                    return traversal.properties(propertyName).as(object.getName());
                } else {
                    return object.isConcrete()
                            ? traversal.values(propertyName).is(object.getLiteralValue())
                            : traversal.values(propertyName).as(object.getName());
                }
        }
    }
}
