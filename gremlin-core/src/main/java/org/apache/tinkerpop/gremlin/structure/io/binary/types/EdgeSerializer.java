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

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.DataType;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

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

        final ReferenceVertex inV = new ReferenceVertex(context.read(buffer),
                                                        context.readValue(buffer, String.class, false));
        final ReferenceVertex outV = new ReferenceVertex(context.read(buffer),
                                                         context.readValue(buffer, String.class, false));

        // discard the parent vertex - we only send "references so this should always be null, but will we change our
        // minds someday????
        context.read(buffer);

        final Map<String, Object> props = context.readValue(buffer, Map.class, true);
        final Edge e = new DetachedEdge(id, label, props,
                outV.id(), outV.label(),
                inV.id(), inV.label()
        );
        return e;
    }

    @Override
    protected void writeValue(final Edge value, final Buffer buffer, final GraphBinaryWriter context) throws IOException {

        context.write(value.id(), buffer);
        context.writeValue(value.label(), buffer, false);

        context.write(value.inVertex().id(), buffer);
        context.writeValue(value.inVertex().label(), buffer, false);
        context.write(value.outVertex().id(), buffer);
        context.writeValue(value.outVertex().label(), buffer, false);

        // we don't serialize the parent Vertex for edges. they are "references", but we leave a place holder
        // here as an option for the future as we've waffled this soooooooooo many times now
        context.write(null, buffer);

        Map<String, Object> props = getPropertiesAsMap(value);
        context.writeValue(props, buffer, true);
    }

    private Map<String, Object> getPropertiesAsMap(Edge e) {
        Map<String, Object> ret = new TreeMap<String, Object>();
        Iterator<? extends Property<Object>> propsIterator = e.properties();
        while(propsIterator.hasNext()) {
            Property<Object> prop = propsIterator.next();
            ret.put(prop.key(), prop.value());
        }
        return ret;
    }
}
