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
package org.apache.tinkerpop.gremlin.structure.util.detached;

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
public class DetachedFactory {

    private DetachedFactory() {
    }

    public static DetachedVertex detach(final Vertex vertex, final boolean withProperties) {
        return vertex instanceof DetachedVertex ? (DetachedVertex) vertex : new DetachedVertex(vertex, withProperties);
    }

    public static DetachedEdge detach(final Edge edge, final boolean withProperties) {
        return edge instanceof DetachedEdge ? (DetachedEdge) edge : new DetachedEdge(edge, withProperties);
    }

    public static <V> DetachedVertexProperty detach(final VertexProperty<V> vertexProperty, final boolean withProperties) {
        return vertexProperty instanceof DetachedVertexProperty ? (DetachedVertexProperty) vertexProperty : new DetachedVertexProperty<>(vertexProperty, withProperties);
    }

    public static <V> DetachedProperty<V> detach(final Property<V> property) {
        return property instanceof DetachedProperty ? (DetachedProperty<V>) property : new DetachedProperty<>(property);
    }

    public static DetachedPath detach(final Path path, final boolean withProperties) {
        return path instanceof DetachedPath ? (DetachedPath) path : new DetachedPath(path, withProperties);
    }

    public static DetachedElement detach(final Element element, final boolean withProperties) {
        if (element instanceof Vertex)
            return detach((Vertex) element, withProperties);
        else if (element instanceof Edge)
            return detach((Edge) element, withProperties);
        else if (element instanceof VertexProperty)
            return detach((VertexProperty) element, withProperties);
        else
            throw new IllegalArgumentException("The provided argument is an unknown element: " + element + ':' + element.getClass());
    }

    public static <D> D detach(final Object object, final boolean withProperties) {
        if (object instanceof Element) {
            return (D) DetachedFactory.detach((Element) object, withProperties);
        } else if (object instanceof Property) {
            return (D) DetachedFactory.detach((Property) object);
        } else if (object instanceof Path) {
            return (D) DetachedFactory.detach((Path) object, withProperties);
        } else if (object instanceof List) {
            final List list = new ArrayList(((List) object).size());
            for (final Object item : (List) object) {
                list.add(DetachedFactory.detach(item, withProperties));
            }
            return (D) list;
        } else if (object instanceof BulkSet) {
            final BulkSet set = new BulkSet();
            for (Map.Entry<Object, Long> entry : ((BulkSet<Object>) object).asBulk().entrySet()) {
                set.add(DetachedFactory.detach(entry.getKey(), withProperties), entry.getValue());
            }
            return (D) set;
        } else if (object instanceof Set) {
            final Set set = object instanceof LinkedHashSet ?
                    new LinkedHashSet(((Set) object).size()) :
                    new HashSet(((Set) object).size());
            for (final Object item : (Set) object) {
                set.add(DetachedFactory.detach(item, withProperties));
            }
            return (D) set;
        } else if (object instanceof Map) {
            final Map map = object instanceof Tree ? new Tree() :
                    object instanceof LinkedHashMap ?
                            new LinkedHashMap(((Map) object).size()) :
                            new HashMap(((Map) object).size());
            for (final Map.Entry<Object, Object> entry : ((Map<Object, Object>) object).entrySet()) {
                map.put(DetachedFactory.detach(entry.getKey(), withProperties), DetachedFactory.detach(entry.getValue(), withProperties));
            }
            return (D) map;
        } else {
            return (D) object;
        }
    }
}
