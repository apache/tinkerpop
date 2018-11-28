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
package org.apache.tinkerpop.gremlin.structure.io.gryo;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.InputShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.KryoShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.OutputShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.SerializerShim;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;

import java.util.Iterator;

/**
 * Set of Gryo 3.1 serializers that are used in addition to (or in overriding fashion for) the ones provided for
 * Gryo 3.0.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class GryoSerializersV3d1 {

    /**
     * Serializes any {@link DetachedVertexProperty} implementation encountered to an {@link DetachedVertexProperty}.
     */
    public final static class DetachedVertexPropertySerializer implements SerializerShim<DetachedVertexProperty> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final DetachedVertexProperty vertexProperty) {
            kryo.writeClassAndObject(output, vertexProperty.id());
            output.writeString(vertexProperty.label());
            kryo.writeClassAndObject(output, vertexProperty.value());
            kryo.writeClassAndObject(output, vertexProperty.element().id());
            output.writeString(vertexProperty.element().label());
            writeElementProperties(kryo, output, vertexProperty);
        }

        @Override
        public <I extends InputShim> DetachedVertexProperty read(final KryoShim<I, ?> kryo, final I input, final Class<DetachedVertexProperty> vertexPropertyClass) {
            final DetachedVertexProperty.Builder vpBuilder = DetachedVertexProperty.build();
            vpBuilder.setId(kryo.readClassAndObject(input));
            vpBuilder.setLabel(input.readString());
            vpBuilder.setValue(kryo.readClassAndObject(input));

            final DetachedVertex.Builder host = DetachedVertex.build();
            host.setId(kryo.readClassAndObject(input));
            host.setLabel(input.readString());
            vpBuilder.setV(host.create());

            while(input.readBoolean()) {
                vpBuilder.addProperty(new DetachedProperty<>(input.readString(), kryo.readClassAndObject(input)));
            }

            return vpBuilder.create();
        }
    }

    private static void writeElementProperties(final KryoShim kryo, final OutputShim output, final Element element) {
        final Iterator<? extends Property> properties = element.properties();
        output.writeBoolean(properties.hasNext());
        while (properties.hasNext()) {
            final Property p = properties.next();
            output.writeString(p.key());
            kryo.writeClassAndObject(output, p.value());
            output.writeBoolean(properties.hasNext());
        }
    }
}
