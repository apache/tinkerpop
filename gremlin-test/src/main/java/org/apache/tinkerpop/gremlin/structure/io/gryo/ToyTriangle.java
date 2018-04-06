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
public final class ToyTriangle {

    private final int x;
    private final int y;
    private final int z;

    public ToyTriangle(final int x, final int y, final int z) {
        this.x = x;
        this.y = y;
        this.z = z;
    }

    public int getX() {
        return this.x;
    }

    public int getY() {
        return this.y;
    }

    public int getZ() {
        return this.z;
    }

    public int hashCode() {
        return this.x + this.y + this.z;
    }

    public boolean equals(final Object other) {
        return other instanceof ToyTriangle && ((ToyTriangle) other).x == this.x && ((ToyTriangle) other).y == this.y && ((ToyTriangle) other).z == this.z;
    }

    @Override
    public String toString() {
        return "[" + this.x + "," + this.y + "," + this.z + "]";
    }

    public static class ToyTriangleSerializer implements SerializerShim<ToyTriangle> {
        @Override
        public <O extends OutputShim> void write(final KryoShim<?, O> kryo, final O output, final ToyTriangle toyTriangle) {
            output.writeInt(toyTriangle.x);
            output.writeInt(toyTriangle.y);
            output.writeInt(toyTriangle.z);
        }

        @Override
        public <I extends InputShim> ToyTriangle read(final KryoShim<I, ?> kryo, final I input, final Class<ToyTriangle> toyTriangleClass) {
            return new ToyTriangle(input.readInt(), input.readInt(), input.readInt());
        }
    }


    public static class ToyTriangleJacksonSerializer extends StdScalarSerializer<ToyTriangle> {

        public ToyTriangleJacksonSerializer() {
            super(ToyTriangle.class);
        }

        @Override
        public void serialize(final ToyTriangle toyTriangle, final JsonGenerator jsonGenerator,
                              final SerializerProvider serializerProvider) throws IOException, JsonGenerationException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeObjectField("x", toyTriangle.x);
            jsonGenerator.writeObjectField("y", toyTriangle.y);
            jsonGenerator.writeObjectField("z", toyTriangle.z);
            jsonGenerator.writeEndObject();
        }
    }

    public static class ToyTriangleJacksonDeSerializer extends AbstractObjectDeserializer<ToyTriangle> {

        public ToyTriangleJacksonDeSerializer() {
            super(ToyTriangle.class);
        }

        @Override
        public ToyTriangle createObject(final Map<String, Object> map) {
            return new ToyTriangle((int) map.get("x"), (int) map.get("y"), (int) map.get("z"));
        }
    }

}
