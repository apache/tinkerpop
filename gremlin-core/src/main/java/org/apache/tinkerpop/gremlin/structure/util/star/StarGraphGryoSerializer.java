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
package org.apache.tinkerpop.gremlin.structure.util.star;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.shaded.ShadedSerializerAdapter;

/**
 * A wrapper for {@link StarGraphSerializer} that makes it compatible with TinkerPop's
 * shaded Kryo.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class StarGraphGryoSerializer extends ShadedSerializerAdapter<StarGraph>  {

    private static final Map<Direction, StarGraphGryoSerializer> CACHE = new EnumMap<>(Direction.class);

    static {
        CACHE.put(Direction.BOTH, new StarGraphGryoSerializer(Direction.BOTH));
        CACHE.put(Direction.IN, new StarGraphGryoSerializer(Direction.IN));
        CACHE.put(Direction.OUT, new StarGraphGryoSerializer(Direction.OUT));
        CACHE.put(null, new StarGraphGryoSerializer(null));
    }

    private StarGraphGryoSerializer(final Direction edgeDirectionToSerialize, final GraphFilter graphFilter) {
        super(new StarGraphSerializer(edgeDirectionToSerialize, graphFilter));
    }

    private StarGraphGryoSerializer(final Direction edgeDirectionToSerialize) {
        this(edgeDirectionToSerialize, new GraphFilter());
    }

    /**
     * Gets a serializer from the cache.  Use {@code null} for the direction when requiring a serializer that
     * doesn't serialize the edges of a vertex.
     */
    public static StarGraphGryoSerializer with(final Direction direction) {
        return CACHE.get(direction);
    }

    public static StarGraphGryoSerializer withGraphFilter(final GraphFilter graphFilter) {
        final StarGraphGryoSerializer serializer = new StarGraphGryoSerializer(Direction.BOTH, graphFilter.clone());
        return serializer;
    }
}
