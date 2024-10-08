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

import org.apache.tinkerpop.gremlin.process.remote.traversal.DefaultRemoteTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.structure.io.binary.DataType;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ListSerializer<T> extends SimpleTypeSerializer<T> {

    // todo to be updated after map serializer changes
    private byte valueFlag = (byte)0x00;
    private static final CollectionSerializer collectionSerializer = new CollectionSerializer(DataType.LIST);

    public ListSerializer() {
        super(DataType.LIST);
    }

    @Override
    public T readValue(final Buffer buffer, final GraphBinaryReader context, final boolean nullable) throws IOException {
        if (nullable) {
            valueFlag = buffer.readByte();
            if ((valueFlag & 1) == 1) {
                return null;
            }
        }

        return readValue(buffer, context);
    }

    @Override
    protected T readValue(final Buffer buffer, final GraphBinaryReader context) throws IOException {
        if ((valueFlag & 2) == 2) {
            final int length = buffer.readInt();

//            if (length == 1) {
//                Object item = context.read(buffer);
//                long bulk = buffer.readLong();
//                return (T) new DefaultRemoteTraverser<>(item, bulk);
//            }

            // need to know that we are bulking traversers in order to deser into traversers else things are expanded
            if (true) {
                final ArrayList result = new ArrayList(length);
                for (int i = 0; i < length; i++) {
                    Object item = context.read(buffer);
                    long bulk = buffer.readLong();
                    result.add(new DefaultRemoteTraverser<>(item, bulk));
                }

                System.out.println("---bulked traverser");
                return (T) result;
            }

            // expanding all bulked results into a list
            final ArrayList result = new ArrayList(length);
            for (int i = 0; i < length; i++) {
                Object item = context.read(buffer);
                long bulk = buffer.readLong();
                for (int j = 0; j < bulk; j++) {
                    result.add(item);
                }
            }

            // return result as bulkset for round tripping? or expand into lists?
//            final BulkSet result = new BulkSet();
//            for (int i = 0; i < length; i++) {
//                result.add(context.read(buffer), buffer.readLong());
//            }

            return (T) result;
        } else {
            // The collection is a List<>
            return (T) collectionSerializer.readValue(buffer, context);
        }
    }

    @Override
    protected void writeValue(final T value, final Buffer buffer, final GraphBinaryWriter context) throws IOException {
//        if (value instanceof Traverser) {
//            Traverser traverser = (Traverser) value;
//            // traverser is single item, so write 1 for size
//            buffer.writeInt(1);
//            context.write(traverser.get(), buffer);
//            buffer.writeLong(traverser.bulk());
//        } else
        if (value instanceof BulkSet) {
            BulkSet bulkSet = (BulkSet) value;
            final Map<Object,Long> raw = bulkSet.asBulk();
            buffer.writeInt(raw.size());

            for (Object key : raw.keySet()) {
                context.write(key, buffer);
                buffer.writeLong(bulkSet.get(key));
            }
        } else {
            collectionSerializer.writeValue((List) value, buffer, context);
        }
    }
}
