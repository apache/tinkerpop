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
package org.apache.tinkerpop.gremlin.structure.io;

import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;

/**
 * Constructs the core {@link Io.Builder} implementations enabling a bit of shorthand syntax by importing these
 * methods statically.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class IoCore {

    private IoCore() {}

    /**
     * Creates a basic GraphML-based {@link org.apache.tinkerpop.gremlin.structure.io.Io.Builder}.
     */
    public static Io.Builder<GraphMLIo> graphml() {
        return GraphMLIo.build();
    }

    /**
     * Creates a basic GraphSON-based {@link org.apache.tinkerpop.gremlin.structure.io.Io.Builder}.
     */
    public static Io.Builder<GraphSONIo> graphson() {
        return GraphSONIo.build();
    }

    /**
     * Creates a basic Gryo-based {@link org.apache.tinkerpop.gremlin.structure.io.Io.Builder}.
     */
    public static Io.Builder<GryoIo> gryo() {
        return GryoIo.build();
    }

    public static Io.Builder createIoBuilder(final String graphFormat) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        final Class<Io.Builder> ioBuilderClass = (Class<Io.Builder>) Class.forName(graphFormat);
        final Io.Builder ioBuilder = ioBuilderClass.newInstance();
        return ioBuilder;
    }
}
