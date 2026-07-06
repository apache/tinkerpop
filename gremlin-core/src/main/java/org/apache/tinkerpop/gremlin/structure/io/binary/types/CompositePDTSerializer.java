/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.structure.io.binary.types;

import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.DataType;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.io.pdt.CompositePDT;

import java.io.IOException;
import java.util.Map;

public class CompositePDTSerializer extends SimpleTypeSerializer<CompositePDT> {

    public CompositePDTSerializer() {
        super(DataType.COMPOSITE_PDT);
    }

    @Override
    protected CompositePDT readValue(final Buffer buffer, final GraphBinaryReader context) throws IOException {
        final String name = context.read(buffer);
        if (name == null || name.isEmpty())
            throw new IOException("CompositePDT name cannot be null or empty");
        final Map<?, ?> fields = context.read(buffer);
        for (final Object key : fields.keySet()) {
            if (!(key instanceof String))
                throw new IOException("CompositePDT fields map must have String keys, found: " + key.getClass().getName());
        }
        @SuppressWarnings("unchecked")
        final Map<String, Object> typedFields = (Map<String, Object>) (Map<?, ?>) fields;
        return new CompositePDT(name, typedFields);
    }

    @Override
    protected void writeValue(final CompositePDT value, final Buffer buffer, final GraphBinaryWriter context) throws IOException {
        context.write(value.getName(), buffer);
        context.write(value.getFields(), buffer);
    }
}
