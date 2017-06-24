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
package org.apache.tinkerpop.gremlin.structure.io.gryo;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedPath;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferencePath;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceProperty;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertexProperty;
import org.apache.tinkerpop.gremlin.util.function.Lambda;

import java.net.InetAddress;
import java.nio.ByteBuffer;

/**
 * {@link AbstractGryoClassResolver} for Gryo 1.0.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GryoClassResolverV1d0 extends AbstractGryoClassResolver {

    @Override
    public Class coerceType(final Class clazz) {
        // force all instances of Vertex, Edge, VertexProperty, etc. to their respective interface
        final Class type;
        if (!ReferenceVertex.class.isAssignableFrom(clazz) && !DetachedVertex.class.isAssignableFrom(clazz) && Vertex.class.isAssignableFrom(clazz))
            type = Vertex.class;
        else if (!ReferenceEdge.class.isAssignableFrom(clazz) && !DetachedEdge.class.isAssignableFrom(clazz) && Edge.class.isAssignableFrom(clazz))
            type = Edge.class;
        else if (!ReferenceVertexProperty.class.isAssignableFrom(clazz) && !DetachedVertexProperty.class.isAssignableFrom(clazz) && VertexProperty.class.isAssignableFrom(clazz))
            type = VertexProperty.class;
        else if (!ReferenceProperty.class.isAssignableFrom(clazz) && !DetachedProperty.class.isAssignableFrom(clazz) && !DetachedVertexProperty.class.isAssignableFrom(clazz) && Property.class.isAssignableFrom(clazz))
            type = Property.class;
        else if (!ReferencePath.class.isAssignableFrom(clazz) && !DetachedPath.class.isAssignableFrom(clazz) && Path.class.isAssignableFrom(clazz))
            type = Path.class;
        else if (Lambda.class.isAssignableFrom(clazz))
            type = Lambda.class;
        else if (ByteBuffer.class.isAssignableFrom(clazz))
            type = ByteBuffer.class;
        else if (Class.class.isAssignableFrom(clazz))
            type = Class.class;
        else if (InetAddress.class.isAssignableFrom(clazz))
            type = InetAddress.class;
        else if (ConnectiveP.class.isAssignableFrom(clazz))
            type = P.class;
        else
            type = clazz;

        return type;
    }
}
