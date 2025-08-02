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

import org.apache.tinkerpop.gremlin.process.traversal.N;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.DataType;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;

import java.io.IOException;
import java.time.MonthDay;

public class NSerializer extends SimpleTypeSerializer<N> {
    public NSerializer() {
        super(DataType.N);
    }

    @Override
    protected N readValue(final Buffer buffer, final GraphBinaryReader context) throws IOException {
        String name = context.read(buffer);
        return name.startsWith("big") ? N.valueOf(name) : N.valueOf(name + "_");
    }

    @Override
    protected void writeValue(final N value, final Buffer buffer, final GraphBinaryWriter context) throws IOException {
        String name = value.name().replace("_", "");
        context.write(name, buffer);
    }
}
