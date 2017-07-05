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
package org.apache.tinkerpop.gremlin.structure.util.reference;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReferenceFactory {

    private ReferenceFactory() {
    }

    public static ReferenceVertex detach(final Vertex vertex) {
        return vertex instanceof ReferenceVertex ? (ReferenceVertex) vertex : new ReferenceVertex(vertex);
    }

    public static ReferenceEdge detach(final Edge edge) {
        return edge instanceof ReferenceEdge ? (ReferenceEdge) edge : new ReferenceEdge(edge);
    }

    public static <V> ReferenceVertexProperty detach(final VertexProperty<V> vertexProperty) {
        return vertexProperty instanceof ReferenceVertexProperty ? (ReferenceVertexProperty) vertexProperty : new ReferenceVertexProperty<>(vertexProperty);
    }

    public static <V> ReferenceProperty<V> detach(final Property<V> property) {
        return property instanceof ReferenceProperty ? (ReferenceProperty) property : new ReferenceProperty<>(property);
    }

    public static ReferencePath detach(final Path path) {
        return path instanceof ReferencePath ? (ReferencePath) path : new ReferencePath(path);
    }

    public static ReferenceElement detach(final Element element) {
        if (element instanceof Vertex)
            return detach((Vertex) element);
        else if (element instanceof Edge)
            return detach((Edge) element);
        else if (element instanceof VertexProperty)
            return detach((VertexProperty) element);
        else
            throw new IllegalArgumentException("The provided argument is an unknown element: " + element + ':' + element.getClass());
    }

    public static <D> D detach(final Object object) {
        if (object instanceof Element) {
            return (D) ReferenceFactory.detach((Element) object);
        } else if (object instanceof Property) {
            return (D) ReferenceFactory.detach((Property) object);
        } else if (object instanceof Path) {
            return (D) ReferenceFactory.detach((Path) object);
        } else if (object instanceof List) {
            final List list = new ArrayList(((List) object).size());
            for (final Object item : (List) object) {
                list.add(ReferenceFactory.detach(item));
            }
            return (D) list;
        } else if (object instanceof BulkSet) {
            final BulkSet set = new BulkSet();
            for (Map.Entry<Object, Long> entry : ((BulkSet<Object>) object).asBulk().entrySet()) {
                set.add(ReferenceFactory.detach(entry.getKey()), entry.getValue());
            }
            return (D) set;
        } else if (object instanceof Set) {
            final Set set = object instanceof LinkedHashSet ?
                    new LinkedHashSet(((Set) object).size()) :
                    new HashSet(((Set) object).size());
            for (final Object item : (Set) object) {
                set.add(ReferenceFactory.detach(item));
            }
            return (D) set;
        } else if (object instanceof Map) {
            final Map map = object instanceof Tree ? new Tree() :
                    object instanceof LinkedHashMap ?
                            new LinkedHashMap(((Map) object).size()) :
                            new HashMap(((Map) object).size());
            for (final Map.Entry<Object, Object> entry : ((Map<Object, Object>) object).entrySet()) {
                map.put(ReferenceFactory.detach(entry.getKey()), ReferenceFactory.detach(entry.getValue()));
            }
            return (D) map;
        } else {
            return (D) object;
        }
    }
}
