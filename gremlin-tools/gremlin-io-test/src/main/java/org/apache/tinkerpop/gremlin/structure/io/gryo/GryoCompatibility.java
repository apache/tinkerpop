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
    V1D0_3_2_7("3.2.7", "1.0", "v1d0"),
    V1D0_3_2_8("3.2.8", "1.0", "v1d0"),
    V1D0_3_2_9("3.2.9", "1.0", "v1d0"),
    V1D0_3_2_10("3.2.10", "1.0", "v1d0"),
    V1D0_3_3_0("3.3.0", "1.0", "v1d0"),
    V3D0_3_3_0("3.3.0", "3.0", "v3d0"),
    V1D0_3_3_1("3.3.1", "1.0", "v1d0"),
    V3D0_3_3_1("3.3.1", "3.0", "v3d0"),
    V1D0_3_3_2("3.3.2", "1.0", "v1d0"),
    V3D0_3_3_2("3.3.2", "3.0", "v3d0"),
    V1D0_3_3_3("3.3.3", "1.0", "v1d0"),
    V3D0_3_3_3("3.3.3", "3.0", "v3d0"),
    V1D0_3_3_4("3.3.4", "1.0", "v1d0"),
    V3D0_3_3_4("3.3.4", "3.0", "v3d0"),
    V1D0_3_3_5("3.3.5", "1.0", "v1d0"),
    V3D0_3_3_5("3.3.5", "3.0", "v3d0"),
    V1D0_3_3_6("3.3.6", "1.0", "v1d0"),
    V3D0_3_3_6("3.3.6", "3.0", "v3d0"),
    V1D0_3_3_7("3.3.7", "1.0", "v1d0"),
    V3D0_3_3_7("3.3.7", "3.0", "v3d0"),
    V1D0_3_3_8("3.3.8", "1.0", "v1d0"),
    V3D0_3_3_8("3.3.8", "3.0", "v3d0"),
    V1D0_3_3_9("3.3.9", "1.0", "v1d0"),
    V3D0_3_3_9("3.3.9", "3.0", "v3d0"),
    V1D0_3_3_10("3.3.10", "1.0", "v1d0"),
    V3D0_3_3_10("3.3.10", "3.0", "v3d0"),
    V1D0_3_3_11("3.3.11", "1.0", "v1d0"),
    V3D0_3_3_11("3.3.11", "3.0", "v3d0"),
    V1D0_3_4_0("3.4.0", "1.0", "v1d0"),
    V3D0_3_4_0("3.4.0", "3.0", "v3d0"),
    V1D0_3_4_1("3.4.1", "1.0", "v1d0"),
    V3D0_3_4_1("3.4.1", "3.0", "v3d0"),
    V1D0_3_4_2("3.4.2", "1.0", "v1d0"),
    V3D0_3_4_2("3.4.2", "3.0", "v3d0"),
    V1D0_3_4_3("3.4.3", "1.0", "v1d0"),
    V3D0_3_4_3("3.4.3", "3.0", "v3d0"),
    V1D0_3_4_4("3.4.4", "1.0", "v1d0"),
    V3D0_3_4_4("3.4.4", "3.0", "v3d0"),
    V1D0_3_4_5("3.4.5", "1.0", "v1d0"),
    V3D0_3_4_5("3.4.5", "3.0", "v3d0"),
    V1D0_3_4_6("3.4.6", "1.0", "v1d0"),
    V3D0_3_4_6("3.4.6", "3.0", "v3d0"),
    V1D0_3_4_7("3.4.7", "1.0", "v1d0"),
    V3D0_3_4_7("3.4.7", "3.0", "v3d0"),
    V1D0_3_4_8("3.4.8", "1.0", "v1d0"),
    V3D0_3_4_8("3.4.8", "3.0", "v3d0"),
    V1D0_3_4_9("3.4.9", "1.0", "v1d0"),
    V3D0_3_4_9("3.4.9", "3.0", "v3d0"),
    V1D0_3_4_10("3.4.10", "1.0", "v1d0"),
    V3D0_3_4_10("3.4.10", "3.0", "v3d0"),
    V1D0_3_4_11("3.4.11", "1.0", "v1d0"),
    V3D0_3_4_11("3.4.11", "3.0", "v3d0"),
    V1D0_3_4_12("3.4.12", "1.0", "v1d0"),
    V3D0_3_4_12("3.4.12", "3.0", "v3d0"),
    V1D0_3_4_13("3.4.13", "1.0", "v1d0"),
    V3D0_3_4_13("3.4.13", "3.0", "v3d0"),

    V1D0_3_5_0("3.5.0", "1.0", "v1d0"),
    V3D0_3_5_0("3.5.0", "3.0", "v3d0"),
    V1D0_3_5_1("3.5.1", "1.0", "v1d0"),
    V3D0_3_5_1("3.5.1", "3.0", "v3d0"),
    V1D0_3_5_2("3.5.2", "1.0", "v1d0"),
    V3D0_3_5_2("3.5.2", "3.0", "v3d0"),

    V1D0_3_6_0("3.6.0", "1.0", "v1d0"),
    V3D0_3_6_0("3.6.0", "3.0", "v3d0");

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
