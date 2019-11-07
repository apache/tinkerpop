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
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;

import java.io.IOException;

public class TreeSerializer extends SimpleTypeSerializer<Tree> {
    public TreeSerializer() {
        super(DataType.TREE);
    }

    @Override
    protected Tree readValue(final Buffer buffer, final GraphBinaryReader context) throws IOException {
        final int length = buffer.readInt();

        final Tree result = new Tree();
        for (int i = 0; i < length; i++) {
            result.put(context.read(buffer), context.readValue(buffer, Tree.class, false));
        }

        return result;
    }

    @Override
    protected void writeValue(final Tree value, final Buffer buffer, final GraphBinaryWriter context) throws IOException {
        buffer.writeInt(value.size());

        for (Object key : value.keySet()) {
            context.write(key, buffer);
            context.writeValue(value.get(key), buffer, false);
        }
    }
}
