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
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;

import java.io.IOException;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class VertexSerializer extends SimpleTypeSerializer<Vertex> {
    public VertexSerializer() {
        super(DataType.VERTEX);
    }

    @Override
    protected Vertex readValue(final Buffer buffer, final GraphBinaryReader context) throws IOException {
        final Vertex v = new ReferenceVertex(context.read(buffer), 
                                             context.readValue(buffer, String.class, false));
        
        // discard the properties - as we only send "references" this should always be null, but will we change our
        // minds some day????
        context.read(buffer);
        return v;
    }

    @Override
    protected void writeValue(final Vertex value, final Buffer buffer, final GraphBinaryWriter context) throws IOException {
        context.write(value.id(), buffer);
        context.writeValue(value.label(), buffer, false);
        context.write(null, buffer);
    }
}
