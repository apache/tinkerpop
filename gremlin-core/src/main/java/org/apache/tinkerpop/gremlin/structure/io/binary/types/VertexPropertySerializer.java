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

import org.apache.commons.collections4.IteratorUtils;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.io.binary.DataType;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertexProperty;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class VertexPropertySerializer extends SimpleTypeSerializer<VertexProperty> {

    public VertexPropertySerializer() {
        super(DataType.VERTEXPROPERTY);
    }

    @Override
    protected VertexProperty readValue(final Buffer buffer, final GraphBinaryReader context) throws IOException {
        final DetachedVertexProperty.Builder builder = DetachedVertexProperty.build()
                .setId(context.read(buffer))
                .setLabel((String) context.readValue(buffer, List.class, false).get(0))
                .setValue(context.read(buffer));

        // discard the parent vertex - we only send "references"
        context.read(buffer);

        final List<Property> properties = context.read(buffer);
        if (properties != null && !properties.isEmpty()) {
            for (Property p : properties) builder.addProperty(p);
        }

        return builder.create();
    }

    @Override
    protected void writeValue(final VertexProperty value, final Buffer buffer, final GraphBinaryWriter context) throws IOException {
        context.write(value.id(), buffer);
        // wrapping label into list here for now according to GraphBinaryV4, but we aren't allowing null label yet
        if (value.label() == null) {
            throw new IOException("Unexpected null value when nullable is false");
        }
        context.writeValue(Collections.singletonList(value.label()), buffer, false);
        context.write(value.value(), buffer);

        // we don't serialize the parent vertex, let's hold a place for it
        context.write(null, buffer);

        if (value instanceof ReferenceVertexProperty) {
            context.write(null, buffer);
        }
        else {
            final List<?> asList = value.graph().features().vertex().supportsMetaProperties() ?
                    IteratorUtils.toList(value.properties()) :
                    Collections.emptyList();
            context.write(asList, buffer);
        }
    }
}
