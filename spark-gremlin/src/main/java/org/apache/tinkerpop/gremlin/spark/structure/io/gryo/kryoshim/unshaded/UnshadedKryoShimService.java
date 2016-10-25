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
package org.apache.tinkerpop.gremlin.spark.structure.io.gryo.kryoshim.unshaded;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.spark.SparkConf;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.spark.structure.io.gryo.IoRegistryAwareKryoSerializer;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoPool;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.KryoShimService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.LinkedBlockingQueue;

public class UnshadedKryoShimService implements KryoShimService {

    private static final Logger log = LoggerFactory.getLogger(UnshadedKryoShimService.class);

    private static final LinkedBlockingQueue<Kryo> KRYOS = new LinkedBlockingQueue<>();

    private static volatile boolean initialized;

    public UnshadedKryoShimService() {
    }

    @Override
    public Object readClassAndObject(final InputStream source) {

        final LinkedBlockingQueue<Kryo> kryos = initialize();

        Kryo k = null;
        try {
            k = kryos.take();

            return k.readClassAndObject(new Input(source));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                kryos.put(k);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void writeClassAndObject(final Object o, OutputStream sink) {

        final LinkedBlockingQueue<Kryo> kryos = initialize();

        Kryo k = null;
        try {
            k = kryos.take();

            final Output kryoOutput = new Output(sink);

            k.writeClassAndObject(kryoOutput, o);

            kryoOutput.flush();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                kryos.put(k);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public int getPriority() {
        return -50;
    }

    @Override
    public void applyConfiguration(final Configuration conf) {
        initialize(conf);
    }

    private LinkedBlockingQueue<Kryo> initialize() {
        return initialize(new BaseConfiguration());
    }

    private LinkedBlockingQueue<Kryo> initialize(final Configuration conf) {
        // DCL is safe in this case due to volatility
        if (!initialized) {
            synchronized (UnshadedKryoShimService.class) {
                if (!initialized) {
                    final SparkConf sparkConf = new SparkConf();

                    // Copy the user's IoRegistry from the param conf to the SparkConf we just created
                    final String regStr = conf.getString(GryoPool.CONFIG_IO_REGISTRY);
                    if (null != regStr) { // SparkConf rejects null values with NPE, so this has to be checked before set(...)
                        sparkConf.set(GryoPool.CONFIG_IO_REGISTRY, regStr);
                    }
                    // Setting spark.serializer here almost certainly isn't necessary, but it doesn't hurt
                    sparkConf.set(Constants.SPARK_SERIALIZER, IoRegistryAwareKryoSerializer.class.getCanonicalName());

                    final String registrator = conf.getString(Constants.SPARK_KRYO_REGISTRATOR);
                    if (null != registrator) {
                        sparkConf.set(Constants.SPARK_KRYO_REGISTRATOR, registrator);
                        log.info("Copied " + Constants.SPARK_KRYO_REGISTRATOR + ": {}", registrator);
                    } else {
                        log.info("Not copying " + Constants.SPARK_KRYO_REGISTRATOR);
                    }

                    // Instantiate the spark.serializer
                    final IoRegistryAwareKryoSerializer ioReg = new IoRegistryAwareKryoSerializer(sparkConf);

                    // Setup a pool backed by our spark.serializer instance
                    // Reuse Gryo poolsize for Kryo poolsize (no need to copy this to SparkConf)
                    final int poolSize = conf.getInt(GryoPool.CONFIG_IO_GRYO_POOL_SIZE,
                            GryoPool.CONFIG_IO_GRYO_POOL_SIZE_DEFAULT);
                    for (int i = 0; i < poolSize; i++) {
                        KRYOS.add(ioReg.newKryo());
                    }

                    initialized = true;
                }
            }
        }

        return KRYOS;
    }
}
