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
package org.apache.tinkerpop.gremlin.spark.structure.io.gryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoPool;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * A {@link KryoSerializer} that attempts to honor {@link GryoPool#CONFIG_IO_REGISTRY}.
 */
public class IoRegistryAwareKryoSerializer extends KryoSerializer {

    private final SparkConf conf;

    private static final Logger log = LoggerFactory.getLogger(IoRegistryAwareKryoSerializer.class);

    public IoRegistryAwareKryoSerializer(final SparkConf conf) {
        super(conf);
        // store conf so that we can access its registry (if one is present) in newKryo()
        this.conf = conf;
    }

    @Override
    public Kryo newKryo() {
        final Kryo kryo = super.newKryo();

        return applyIoRegistryIfPresent(kryo);
    }

    private Kryo applyIoRegistryIfPresent(final Kryo kryo) {
        if (!conf.contains(GryoPool.CONFIG_IO_REGISTRY)) {
            log.info("SparkConf {} does not contain setting {}, skipping {} handling",
                    GryoPool.CONFIG_IO_REGISTRY, conf, IoRegistry.class.getCanonicalName());
            return kryo;
        }

        final String registryClassnames = conf.get(GryoPool.CONFIG_IO_REGISTRY);

        for (String registryClassname : registryClassnames.split(",")) {
            final IoRegistry registry;

            try {
                registry = (IoRegistry) Class.forName(registryClassname).newInstance();
                log.info("Instantiated {}", registryClassname);
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                log.error("Unable to reflectively instantiate the {} implementation named {}",
                        IoRegistry.class.getCanonicalName(), registryClassname, e);
                return kryo;
            }

            // Left is the class targeted for serialization, right is a mess of potential types, including
            // a shaded Serializer impl, unshaded Serializer impl, or Function<shaded.Kryo,shaded.Serializer>
            final List<Pair<Class, Object>> serializers = registry.find(GryoIo.class);

            if (null == serializers) {
                log.info("Invoking find({}.class) returned null on registry {}; ignoring this registry",
                        GryoIo.class.getCanonicalName(), registry);
                return kryo;
            }

            for (Pair<Class, Object> p : serializers) {
                if (null == p.getValue1()) {
                    // null on the right is fine
                    log.info("Registering {} with default serializer", p.getValue0());
                    kryo.register(p.getValue0());
                } else if (p.getValue1() instanceof Serializer) {
                    // unshaded serializer on the right is fine
                    log.info("Registering {} with serializer {}", p.getValue0(), p.getValue1());
                    kryo.register(p.getValue0(), (Serializer) p.getValue1());
                } else {
                    // anything else on the right is unsupported with Spark
                    log.error("Serializer {} found in {} must implement {} " +
                                    "(the shaded interface {} is not supported on Spark).  This class will be registered with " +
                                    "the default behavior of Spark's KryoSerializer.",
                            p.getValue1(), registryClassname, Serializer.class.getCanonicalName(),
                            org.apache.tinkerpop.shaded.kryo.Serializer.class.getCanonicalName());
                    kryo.register(p.getValue0());
                }
            }
        }

        return kryo;
    }
}
