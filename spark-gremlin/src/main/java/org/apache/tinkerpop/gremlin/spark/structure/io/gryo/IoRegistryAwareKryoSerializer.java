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

/**
 * Copyright DataStax, Inc.
 * <p>
 * Please see the included license file for details.
 */
package org.apache.tinkerpop.gremlin.spark.structure.io.gryo;

import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.tinkerpop.gremlin.spark.structure.io.gryo.kryoshim.unshaded.UnshadedSerializerAdapter;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoPool;
import org.apache.tinkerpop.gremlin.structure.io.gryo.TypeRegistration;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.shaded.ShadedSerializerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A {@link KryoSerializer} that attempts to honor {@link GryoPool#CONFIG_IO_REGISTRY}.
 */
public final class IoRegistryAwareKryoSerializer extends KryoSerializer {

    private static final Logger log = LoggerFactory.getLogger(IoRegistryAwareKryoSerializer.class);

    private final List<TypeRegistration<?>> typeRegistrations = new ArrayList<>();

    public IoRegistryAwareKryoSerializer(final SparkConf configuration) {
        super(configuration);
        if (!configuration.contains(GryoPool.CONFIG_IO_REGISTRY))
            log.info("SparkConf does not contain a {} property. Skipping {} processing.", GryoPool.CONFIG_IO_REGISTRY, IoRegistry.class.getCanonicalName());
        else {
            final GryoPool pool = GryoPool.build().poolSize(1).ioRegistries(Arrays.asList(configuration.get(GryoPool.CONFIG_IO_REGISTRY).split(","))).create();
            for (final TypeRegistration<?> type : pool.getMapper().getTypeRegistrations()) {
                log.info("Registering {} with serializer type: {}", type.getTargetClass().getCanonicalName(), type);
                this.typeRegistrations.add(type);
            }
        }
    }

    @Override
    public Kryo newKryo() {
        final Kryo kryo = super.newKryo();
        for (final TypeRegistration<?> type : this.typeRegistrations) {
            if (null != type.getSerializerShim())
                kryo.register(type.getTargetClass(), new UnshadedSerializerAdapter(type.getSerializerShim()), type.getId());
            else if (null != type.getShadedSerializer() && type.getShadedSerializer() instanceof ShadedSerializerAdapter)
                kryo.register(type.getTargetClass(), new UnshadedSerializerAdapter(((ShadedSerializerAdapter) type.getShadedSerializer()).getSerializerShim()), type.getId());
            else
                kryo.register(type.getTargetClass(), type.getId());
        }
        return kryo;
    }
}
