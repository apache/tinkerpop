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

import org.apache.commons.io.IOUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.Compatibility;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedPath;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;

import java.io.File;
import java.io.IOException;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public enum GryoCompatibility implements Compatibility {
    V1D0_3_2_3("3.2.3", "1.0", "v1d0"),
    V1D0_3_2_4("3.2.4", "1.0", "v1d0"),
    V1D0_3_2_5("3.2.5", "1.0", "v1d0"),
    V1D0_3_2_6("3.2.6", "1.0", "v1d0"),
    V1D0_3_3_0("3.3.0", "1.0", "v1d0"),
    V3D0_3_3_0("3.3.0", "3.0", "v3d0"),
    V1D0_3_3_1("3.3.0", "1.0", "v1d0"),
    V3D0_3_3_1("3.3.1", "3.0", "v3d0");

    private static final String SEP = File.separator;

    private final String gryoVersion;
    private final String tinkerpopVersion;
    private final String configuration;

    GryoCompatibility(final String tinkerpopVersion, final String gryoVersion, final String configuration) {
        this.tinkerpopVersion = tinkerpopVersion;
        this.gryoVersion = gryoVersion;
        this.configuration = configuration;
    }

    @Override
    public byte[] readFromResource(final String resource) throws IOException {
        final String testResource = "_" + tinkerpopVersion.replace(".", "_") + SEP + resource + "-" + configuration + ".kryo";
        return IOUtils.toByteArray(getClass().getResourceAsStream(testResource));
    }

    @Override
    public Class resolve(final Class clazz) {
        if (clazz.equals(Edge.class))
            return DetachedEdge.class;
        else if (clazz.equals(Vertex.class))
            return DetachedVertex.class;
        else if (clazz.equals(Property.class))
            return DetachedProperty.class;
        else if (clazz.equals(VertexProperty.class))
            return DetachedVertexProperty.class;
        else if (clazz.equals(Path.class))
            return DetachedPath.class;
        else if (clazz.equals(TraversalMetrics.class))
            return DefaultTraversalMetrics.class;
        else if (clazz.equals(Metrics.class))
            return MutableMetrics.class;
        else if (clazz.equals(Traverser.class))
            return B_O_Traverser.class;
        else
            return clazz;
    }

    @Override
    public String getReleaseVersion() {
        return tinkerpopVersion;
    }

    @Override
    public String getVersion() {
        return gryoVersion;
    }

    @Override
    public String getConfiguration() {
        return configuration;
    }

    @Override
    public String toString() {
        return tinkerpopVersion + "-" + configuration;
    }
}
