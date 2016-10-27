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
import org.apache.commons.configuration.Configuration;
import org.apache.spark.SparkConf;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.spark.structure.Spark;
import org.apache.tinkerpop.gremlin.spark.structure.io.gryo.IoRegistryAwareKryoSerializer;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoPool;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.KryoShimService;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.LinkedBlockingQueue;

public class UnshadedKryoShimService implements KryoShimService {

    private static final LinkedBlockingQueue<Kryo> KRYOS = new LinkedBlockingQueue<>();
    private static volatile boolean INITIALIZED;

    @Override
    public Object readClassAndObject(final InputStream inputStream) {
        Kryo k = null;
        try {
            k = KRYOS.take();
            return k.readClassAndObject(new Input(inputStream));
        } catch (final InterruptedException e) {
            throw new IllegalStateException(e);
        } finally {
            try {
                KRYOS.put(k);
            } catch (final InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    @Override
    public void writeClassAndObject(final Object object, OutputStream outputStream) {
        Kryo k = null;
        try {
            k = KRYOS.take();
            final Output kryoOutput = new Output(outputStream);
            k.writeClassAndObject(kryoOutput, object);
            kryoOutput.flush();
        } catch (final InterruptedException e) {
            throw new IllegalStateException(e);
        } finally {
            try {
                KRYOS.put(k);
            } catch (final InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    @Override
    public int getPriority() {
        return -50;
    }

    @Override
    public void applyConfiguration(final Configuration configuration) {
        initialize(configuration);
    }

    @Override
    public void close() {
        INITIALIZED = false;
        KRYOS.clear();
    }

    private LinkedBlockingQueue<Kryo> initialize(final Configuration configuration) {
        // DCL is safe in this case due to volatility
        if (!INITIALIZED) {
            synchronized (UnshadedKryoShimService.class) {
                if (!INITIALIZED) {
                    // so we don't get a WARN that a new configuration is being created within an active context
                    final SparkConf sparkConf = null == Spark.getContext() ? new SparkConf() : Spark.getContext().getConf().clone();
                    configuration.getKeys().forEachRemaining(key -> sparkConf.set(key, configuration.getProperty(key).toString()));
                    // Setting spark.serializer here almost certainly isn't necessary, but it doesn't hurt
                    sparkConf.set(Constants.SPARK_SERIALIZER, IoRegistryAwareKryoSerializer.class.getCanonicalName());
                    // Instantiate the spark.serializer
                    final IoRegistryAwareKryoSerializer ioRegistrySerializer = new IoRegistryAwareKryoSerializer(sparkConf);
                    // Setup a pool backed by our spark.serializer instance
                    // Reuse Gryo poolsize for Kryo poolsize (no need to copy this to SparkConf)
                    KRYOS.clear();
                    final int poolSize = configuration.getInt(GryoPool.CONFIG_IO_GRYO_POOL_SIZE, GryoPool.CONFIG_IO_GRYO_POOL_SIZE_DEFAULT);
                    for (int i = 0; i < poolSize; i++) {
                        KRYOS.add(ioRegistrySerializer.newKryo());
                    }
                    INITIALIZED = true;
                }
            }
        }

        return KRYOS;
    }
}
