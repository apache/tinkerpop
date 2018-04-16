/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.structure.io.gryo;

import org.apache.tinkerpop.gremlin.structure.io.graphson.AbstractObjectDeserializer;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.InputShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.KryoShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.OutputShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.SerializerShim;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerationException;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdScalarSerializer;

import java.io.IOException;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ToyPoint {

    private final int x;
    private final int y;

    public ToyPoint(final int x, final int y) {
        this.x = x;
        this.y = y;
    }

    public int getX() {
        return this.x;
    }

    public int getY() {
        return this.y;
    }

    public int hashCode() {
        return this.x + this.y;
    }

    public boolean equals(final Object other) {
        return other instanceof ToyPoint && ((ToyPoint) other).x == this.x && ((ToyPoint) other).y == this.y;
    }

    @Override
    public String toString() {
        return "[" + this.x + "," + this.y + "]";
    }

    public static class ToyPointSerializer implements SerializerShim<ToyPoint> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final ToyPoint toyPoint) {
            output.writeInt(toyPoint.x);
            output.writeInt(toyPoint.y);
        }

        @Override
        public <I extends InputShim> ToyPoint read(final KryoShim<I, ?> kryo, final I input, final Class<ToyPoint> toyPointClass) {
            return new ToyPoint(input.readInt(), input.readInt());
        }
    }

    public static class ToyPointJacksonSerializer extends StdScalarSerializer<ToyPoint> {

        public ToyPointJacksonSerializer() {
            super(ToyPoint.class);
        }

        @Override
        public void serialize(final ToyPoint toyPoint, final JsonGenerator jsonGenerator,
                              final SerializerProvider serializerProvider) throws IOException, JsonGenerationException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeObjectField("x", toyPoint.x);
            jsonGenerator.writeObjectField("y", toyPoint.y);
            jsonGenerator.writeEndObject();
        }
    }

    public static class ToyPointJacksonDeSerializer extends AbstractObjectDeserializer<ToyPoint> {

        public ToyPointJacksonDeSerializer() {
            super(ToyPoint.class);
        }

        @Override
        public ToyPoint createObject(final Map<String, Object> map) {
            return new ToyPoint((int) map.get("x"), (int) map.get("y"));
        }
    }
}
