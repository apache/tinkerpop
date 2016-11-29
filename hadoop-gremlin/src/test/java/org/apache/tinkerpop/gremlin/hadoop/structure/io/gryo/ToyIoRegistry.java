/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo;

import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TinkerPopJacksonModule;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ToyIoRegistry extends AbstractIoRegistry {

    private static final ToyIoRegistry INSTANCE = new ToyIoRegistry();

    private ToyIoRegistry() {
        super.register(GryoIo.class, ToyPoint.class, new ToyPoint.ToyPointSerializer());
        super.register(GryoIo.class, ToyTriangle.class, new ToyTriangle.ToyTriangleSerializer());
        super.register(GraphSONIo.class, null, new ToyModule());
    }

    public static class ToyModule extends TinkerPopJacksonModule {
        public ToyModule() {
            super("toy");
            addSerializer(ToyPoint.class, new ToyPoint.ToyPointJacksonSerializer());
            addDeserializer(ToyPoint.class, new ToyPoint.ToyPointJacksonDeSerializer());
            addSerializer(ToyTriangle.class, new ToyTriangle.ToyTriangleJacksonSerializer());
            addDeserializer(ToyTriangle.class, new ToyTriangle.ToyTriangleJacksonDeSerializer());
        }


        @Override
        public Map<Class, String> getTypeDefinitions() {
            return new HashMap<Class, String>() {{
                put(ToyPoint.class, "ToyPoint");
                put(ToyTriangle.class, "ToyTriangle");
            }};
        }

        @Override
        public String getTypeNamespace() {
            return "toy";
        }
    }

    public static ToyIoRegistry instance() {
        return INSTANCE;
    }
}