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
import org.apache.tinkerpop.gremlin.process.traversal.Pick;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.SackFunctions;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
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
import org.apache.tinkerpop.shaded.jackson.core.JsonToken;
import org.apache.tinkerpop.shaded.jackson.core.type.WritableTypeId;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeIdResolver;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * GraphSON 4.0 {@code TypeSerializer}.
 *
 * @author Kevin Gallardo (https://kgdo.me)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphSONTypeSerializerV4 extends AbstractGraphSONTypeSerializer {

    GraphSONTypeSerializerV4(final TypeIdResolver idRes, final String propertyName, final TypeInfo typeInfo,
                             final String valuePropertyName) {
        super(idRes, propertyName, typeInfo, valuePropertyName);
    }

    @Override
    public WritableTypeId writeTypePrefix(final JsonGenerator jsonGenerator, final WritableTypeId writableTypeId) throws IOException {
        if (writableTypeId.valueShape == JsonToken.START_OBJECT) {
            if (writableTypeId.forValue instanceof Map) {
                writeTypePrefix(jsonGenerator, getTypeIdResolver().idFromValueAndType(writableTypeId.forValue, getClassFromObject(writableTypeId.forValue)));
                jsonGenerator.writeStartArray();
            } else {
                jsonGenerator.writeStartObject();
            }
        } else if (writableTypeId.valueShape == JsonToken.START_ARRAY) {
            if (writableTypeId.forValue instanceof List || writableTypeId.forValue instanceof Set) {
                writeTypePrefix(jsonGenerator, getTypeIdResolver().idFromValueAndType(writableTypeId.forValue, getClassFromObject(writableTypeId.forValue)));
                jsonGenerator.writeStartArray();
            } else {
                jsonGenerator.writeStartArray();
            }
        } else if (canWriteTypeId()) {
            writeTypePrefix(jsonGenerator, getTypeIdResolver().idFromValueAndType(writableTypeId.forValue, getClassFromObject(writableTypeId.forValue)));
        } else {
            throw new IllegalStateException("Could not write prefix: shape[" + writableTypeId.valueShape + "] value[" + writableTypeId.forValue + "]");
        }

        return writableTypeId;
    }

    @Override
    public WritableTypeId writeTypeSuffix(final JsonGenerator jsonGenerator, final WritableTypeId writableTypeId) throws IOException {
        if (writableTypeId.valueShape == JsonToken.START_OBJECT) {
            if (writableTypeId.forValue instanceof Map) {
                jsonGenerator.writeEndArray();
                writeTypeSuffix(jsonGenerator);
            } else {
                jsonGenerator.writeEndObject();
            }
        } else if (writableTypeId.valueShape == JsonToken.START_ARRAY) {
            if (writableTypeId.forValue instanceof List || writableTypeId.forValue instanceof Set) {
                jsonGenerator.writeEndArray();
                writeTypeSuffix(jsonGenerator);
            } else {
                jsonGenerator.writeEndArray();
            }
        } else if (canWriteTypeId()) {
            writeTypeSuffix(jsonGenerator);
        } else {
            throw new IllegalStateException("Could not write suffix: shape[" + writableTypeId.valueShape + "] value[" + writableTypeId.forValue + "]");
        }

        return writableTypeId;
    }

    @Override
    protected Class getClassFromObject(final Object o) {
        final Class c = o.getClass();
        if (classMap.containsKey(c))
            return classMap.get(c);

        final Class mapped;
        if (Map.class.isAssignableFrom(c)) {
            if (Tree.class.isAssignableFrom(c))
                mapped = Tree.class;
            else
                mapped = Map.class;
        } else if (List.class.isAssignableFrom(c))
            mapped = List.class;
        else if (Set.class.isAssignableFrom(c) && !BulkSet.class.isAssignableFrom(c))   // we have special handling of BulkSet
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
        else if (Pick.class.isAssignableFrom(c))
            mapped = Pick.class;
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
