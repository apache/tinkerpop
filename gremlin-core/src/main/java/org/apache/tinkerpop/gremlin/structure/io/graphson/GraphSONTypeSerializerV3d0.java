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
package org.apache.tinkerpop.gremlin.structure.io.graphson;

import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.SackFunctions;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeIdResolver;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * GraphSON 2.0 {@code TypeSerializer}.
 *
 * @author Kevin Gallardo (https://kgdo.me)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphSONTypeSerializerV3d0 extends AbstractGraphSONTypeSerializer {

    GraphSONTypeSerializerV3d0(final TypeIdResolver idRes, final String propertyName, final TypeInfo typeInfo,
                               final String valuePropertyName) {
        super(idRes, propertyName, typeInfo, valuePropertyName);
    }

    @Override
    public void writeTypePrefixForObject(final Object o, final JsonGenerator jsonGenerator) throws IOException {
        if (o instanceof Map) {
            writeTypePrefix(jsonGenerator, getTypeIdResolver().idFromValueAndType(o, getClassFromObject(o)));
            jsonGenerator.writeStartArray();
        } else {
            jsonGenerator.writeStartObject();
        }
    }

    @Override
    public void writeTypeSuffixForObject(final Object o, final JsonGenerator jsonGenerator) throws IOException {
        if (o instanceof Map) {
            jsonGenerator.writeEndArray();
            writeTypeSuffix(jsonGenerator);
        } else {
            jsonGenerator.writeEndObject();
        }
    }

    @Override
    public void writeTypePrefixForArray(final Object o, final JsonGenerator jsonGenerator) throws IOException {
        if (o instanceof List || o instanceof Set) {
            writeTypePrefix(jsonGenerator, getTypeIdResolver().idFromValueAndType(o, getClassFromObject(o)));
            jsonGenerator.writeStartArray();
        } else {
            jsonGenerator.writeStartArray();
        }
    }

    @Override
    public void writeTypeSuffixForArray(final Object o, final JsonGenerator jsonGenerator) throws IOException {
        if (o instanceof List || o instanceof Set) {
            jsonGenerator.writeEndArray();
            writeTypeSuffix(jsonGenerator);
        } else {
            jsonGenerator.writeEndArray();
        }
    }

    @Override
    protected Class getClassFromObject(final Object o) {
        final Class c = o.getClass();
        if (classMap.containsKey(c))
            return classMap.get(c);

        final Class mapped;
        if (Map.class.isAssignableFrom(c))
            mapped = Map.class;
        else if (List.class.isAssignableFrom(c))
            mapped = List.class;
        else if (Set.class.isAssignableFrom(c))
            mapped = Set.class;
        else if (Vertex.class.isAssignableFrom(c))
            mapped = Vertex.class;
        else if (Edge.class.isAssignableFrom(c))
            mapped = Edge.class;
        else if (Path.class.isAssignableFrom(c))
            mapped = Path.class;
        else if (VertexProperty.class.isAssignableFrom(c))
            mapped = VertexProperty.class;
        else if (Metrics.class.isAssignableFrom(c))
            mapped = Metrics.class;
        else if (TraversalMetrics.class.isAssignableFrom(c))
            mapped = TraversalMetrics.class;
        else if (Property.class.isAssignableFrom(c))
            mapped = Property.class;
        else if (ByteBuffer.class.isAssignableFrom(c))
            mapped = ByteBuffer.class;
        else if (InetAddress.class.isAssignableFrom(c))
            mapped = InetAddress.class;
        else if (Traverser.class.isAssignableFrom(c))
            mapped = Traverser.class;
        else if (Lambda.class.isAssignableFrom(c))
            mapped = Lambda.class;
        else if (VertexProperty.Cardinality.class.isAssignableFrom(c))
            mapped = VertexProperty.Cardinality.class;
        else if (Column.class.isAssignableFrom(c))
            mapped = Column.class;
        else if (Direction.class.isAssignableFrom(c))
            mapped = Direction.class;
        else if (Operator.class.isAssignableFrom(c))
            mapped = Operator.class;
        else if (Order.class.isAssignableFrom(c))
            mapped = Order.class;
        else if (Pop.class.isAssignableFrom(c))
            mapped = Pop.class;
        else if (SackFunctions.Barrier.class.isAssignableFrom(c))
            mapped = SackFunctions.Barrier.class;
        else if (TraversalOptionParent.Pick.class.isAssignableFrom(c))
            mapped = TraversalOptionParent.Pick.class;
        else if (Scope.class.isAssignableFrom(c))
            mapped = Scope.class;
        else if (T.class.isAssignableFrom(c))
            mapped = T.class;
        else
            mapped = c;

        classMap.put(c, mapped);
        return mapped;
    }
}
