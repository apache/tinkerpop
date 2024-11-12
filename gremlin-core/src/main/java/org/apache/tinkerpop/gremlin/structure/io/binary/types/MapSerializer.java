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
package org.apache.tinkerpop.gremlin.structure.io.binary.types;

import org.apache.tinkerpop.gremlin.structure.io.binary.DataType;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class MapSerializer extends SimpleTypeSerializer<Map> {

    public MapSerializer() {
        super(DataType.MAP);
    }

    @Override
    public Map readValue(final Buffer buffer, final GraphBinaryReader context, final boolean nullable) throws IOException {
        if (nullable) {
            final byte valueFlag = buffer.readByte();
            if ((valueFlag & 1) == 1) {
                return null;
            }
            if ((valueFlag & 2) == 2) {
                return readValue(buffer, context);
            }

        }

        return readMap(buffer, context);
    }

    @Override
    protected Map readValue(final Buffer buffer, final GraphBinaryReader context) throws IOException {
        final int length = buffer.readInt();

        final Map<Object,Object> result = new LinkedHashMap<>(length);
        for (int i = 0; i < length; i++) {
            result.put(context.read(buffer), context.read(buffer));
        }

        return result;
    }

    private Map readMap(final Buffer buffer, final GraphBinaryReader context) throws IOException {
        final int length = buffer.readInt();

        final Map<Object,Object> result = new HashMap<>(length);
        for (int i = 0; i < length; i++) {
            result.put(context.read(buffer), context.read(buffer));
        }

        return result;
    }

    @Override
    protected void writeValue(final Map value, final Buffer buffer, final GraphBinaryWriter context) throws IOException {
        buffer.writeInt(value.size());

        for (Map.Entry entry : (Set<Map.Entry>) value.entrySet()) {
            context.write(entry.getKey(), buffer);
            context.write(entry.getValue(), buffer);
        }
    }
}
