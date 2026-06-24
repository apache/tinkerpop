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
package org.apache.tinkerpop.gremlin.structure.io.graphson;

import org.apache.tinkerpop.gremlin.structure.io.pdt.ProviderDefinedTypeRegistry;
import org.apache.tinkerpop.shaded.jackson.databind.JsonSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.SerializationConfig;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.ser.DefaultSerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.ser.SerializerFactory;

/**
 * A {@link DefaultSerializerProvider} for GraphSON V4 that returns a PDT adapter-based serializer
 * for classes registered in the {@link ProviderDefinedTypeRegistry}.
 */
final class PdtGraphSONSerializerProviderV4 extends DefaultSerializerProvider {
    private static final long serialVersionUID = 1L;
    private final ProviderDefinedTypeRegistry pdtRegistry;
    private final JsonSerializer<Object> pdtAdapterSerializer;

    PdtGraphSONSerializerProviderV4(final ProviderDefinedTypeRegistry pdtRegistry) {
        super();
        this.pdtRegistry = pdtRegistry;
        this.pdtAdapterSerializer = new PdtGraphSONSerializersV4.PdtAdapterJacksonSerializer(pdtRegistry);
    }

    private PdtGraphSONSerializerProviderV4(final SerializerProvider src,
                                            final SerializationConfig config, final SerializerFactory f,
                                            final ProviderDefinedTypeRegistry pdtRegistry,
                                            final JsonSerializer<Object> pdtAdapterSerializer) {
        super(src, config, f);
        this.pdtRegistry = pdtRegistry;
        this.pdtAdapterSerializer = pdtAdapterSerializer;
    }

    @Override
    public JsonSerializer<Object> getUnknownTypeSerializer(final Class<?> aClass) {
        if (pdtRegistry != null && pdtRegistry.getAdapterByClass(aClass).isPresent()) {
            return pdtAdapterSerializer;
        }
        return super.getUnknownTypeSerializer(aClass);
    }

    @Override
    public PdtGraphSONSerializerProviderV4 createInstance(final SerializationConfig config,
                                                          final SerializerFactory jsf) {
        return new PdtGraphSONSerializerProviderV4(this, config, jsf, pdtRegistry, pdtAdapterSerializer);
    }
}
