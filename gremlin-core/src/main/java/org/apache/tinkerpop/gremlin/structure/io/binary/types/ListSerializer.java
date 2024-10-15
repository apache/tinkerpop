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
import java.util.ArrayList;
import java.util.List;

public class ListSerializer extends SimpleTypeSerializer<List> {
    private static final CollectionSerializer collectionSerializer = new CollectionSerializer(DataType.LIST);

    public ListSerializer() {
        super(DataType.LIST);
    }

    @Override
    public List readValue(final Buffer buffer, final GraphBinaryReader context, final boolean nullable) throws IOException {
        if (nullable) {
            final byte valueFlag = buffer.readByte();
            if ((valueFlag & 1) == 1) {
                return null;
            }
            if ((valueFlag & 2) == 2) {
                final int length = buffer.readInt();
                final ArrayList result = new ArrayList(length);
                for (int i = 0; i < length; i++) {
                    Object item = context.read(buffer);
                    long bulk = buffer.readLong();
                    for (int j = 0; j < bulk; j++) {
                        result.add(item);
                    }
                }
                return result;
            }
        }

        return readValue(buffer, context);
    }

    @Override
    protected List readValue(final Buffer buffer, final GraphBinaryReader context) throws IOException {
        // The collection is a List<>
        return (List) collectionSerializer.readValue(buffer, context);
    }

    @Override
    protected void writeValue(final List value, final Buffer buffer, final GraphBinaryWriter context) throws IOException {
        collectionSerializer.writeValue(value, buffer, context);
    }
}
