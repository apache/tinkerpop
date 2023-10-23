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

import org.apache.commons.collections.IteratorUtils;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.DataType;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;

import java.io.IOException;
import java.util.List;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class EdgeSerializer extends SimpleTypeSerializer<Edge> {
    public EdgeSerializer() {
        super(DataType.EDGE);
    }

    @Override
    protected Edge readValue(final Buffer buffer, final GraphBinaryReader context) throws IOException {
        final Object id = context.read(buffer);
        final String label = context.readValue(buffer, String.class, false);

        final Object inVId = context.read(buffer);
        final String inVLabel = context.readValue(buffer, String.class, false);
        final Object outVId = context.read(buffer);
        final String outVLabel = context.readValue(buffer, String.class, false);

        // discard the parent vertex
        context.read(buffer);

        final List<Property> properties = context.read(buffer);

        final DetachedVertex inV = DetachedVertex.build().setId(inVId).setLabel(inVLabel).create();
        final DetachedVertex outV = DetachedVertex.build().setId(outVId).setLabel(outVLabel).create();

        final DetachedEdge.Builder builder = DetachedEdge.build().setId(id).setLabel(label).setInV(inV).setOutV(outV);

        if (properties != null) {
            for (final Property p : properties) {
                builder.addProperty(p);
            }
        }

        return builder.create();
    }

    @Override
    protected void writeValue(final Edge value, final Buffer buffer, final GraphBinaryWriter context) throws IOException {

        context.write(value.id(), buffer);
        context.writeValue(value.label(), buffer, false);

        context.write(value.inVertex().id(), buffer);
        context.writeValue(value.inVertex().label(), buffer, false);
        context.write(value.outVertex().id(), buffer);
        context.writeValue(value.outVertex().label(), buffer, false);

        // we don't serialize the parent Vertex for edges.
        context.write(null, buffer);
        if (value.properties() == null) {
            context.write(null, buffer);
        }
        else {
            final List<?> asList = IteratorUtils.toList(value.properties());
            context.write(asList, buffer);
        }
    }
}
