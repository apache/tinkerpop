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

import org.apache.commons.collections4.IteratorUtils;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.DataType;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

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
        // Read all labels as List<String> for multi-label support
        final List<String> labelList = context.readValue(buffer, List.class, false);

        final Object inVId = context.read(buffer);
        // Read all inVertex labels as List<String> for multi-label support
        final List<String> inVLabelList = context.readValue(buffer, List.class, false);
        final Object outVId = context.read(buffer);
        // Read all outVertex labels as List<String> for multi-label support
        final List<String> outVLabelList = context.readValue(buffer, List.class, false);

        // discard the parent vertex
        context.read(buffer);

        final List<Property> properties = context.read(buffer);

        final DetachedVertex.Builder inVBuilder = DetachedVertex.build().setId(inVId);
        if (inVLabelList.size() == 1) {
            inVBuilder.setLabel(inVLabelList.get(0));
        } else if (!inVLabelList.isEmpty()) {
            inVBuilder.setLabels(new LinkedHashSet<>(inVLabelList));
        }
        final DetachedVertex inV = inVBuilder.create();

        final DetachedVertex.Builder outVBuilder = DetachedVertex.build().setId(outVId);
        if (outVLabelList.size() == 1) {
            outVBuilder.setLabel(outVLabelList.get(0));
        } else if (!outVLabelList.isEmpty()) {
            outVBuilder.setLabels(new LinkedHashSet<>(outVLabelList));
        }
        final DetachedVertex outV = outVBuilder.create();

        final DetachedEdge.Builder builder = DetachedEdge.build().setId(id).setInV(inV).setOutV(outV);
        if (labelList.size() == 1) {
            builder.setLabel(labelList.get(0));
        } else if (!labelList.isEmpty()) {
            builder.setLabels(new LinkedHashSet<>(labelList));
        }

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
        // Write all edge labels as List<String> for multi-label support
        final Set<String> edgeLabels = value.labels();
        if (edgeLabels == null || edgeLabels.isEmpty()) {
            throw new IOException("Unexpected null or empty labels when nullable is false");
        }
        context.writeValue(new ArrayList<>(edgeLabels), buffer, false);

        context.write(value.inVertex().id(), buffer);
        // Write all inVertex labels as List<String> for multi-label support
        final Set<String> inVLabels = value.inVertex().labels();
        if (inVLabels == null || inVLabels.isEmpty()) {
            throw new IOException("Unexpected null or empty labels when nullable is false");
        }
        context.writeValue(new ArrayList<>(inVLabels), buffer, false);

        context.write(value.outVertex().id(), buffer);
        // Write all outVertex labels as List<String> for multi-label support
        final Set<String> outVLabels = value.outVertex().labels();
        if (outVLabels == null || outVLabels.isEmpty()) {
            throw new IOException("Unexpected null or empty labels when nullable is false");
        }
        context.writeValue(new ArrayList<>(outVLabels), buffer, false);

        // we don't serialize the parent Vertex for edges.
        context.write(null, buffer);
        if (value instanceof ReferenceEdge) {
            context.write(null, buffer);
        }
        else {
            final List<?> asList = IteratorUtils.toList(value.properties());
            context.write(asList, buffer);
        }
    }
}
