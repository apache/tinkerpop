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

import com.twitter.chill.KryoInstantiator;
import com.twitter.chill.KryoPool;
import com.twitter.chill.SerDeState;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.tinkerpop.gremlin.spark.structure.io.TinkerPopKryoRegistrator;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.KryoShimService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class UnshadedKryoShimService implements KryoShimService {

    public static final String SPARK_KRYO_POOL_SIZE_SYSTEM_PROPERTY = "tinkerpop.kryo.poolsize";

    private static final Logger log = LoggerFactory.getLogger(UnshadedKryoShimService.class);
    private static final int SPARK_KRYO_POOL_SIZE_DEFAULT = 8;

    private final KryoSerializer sparkKryoSerializer;
    private final KryoPool kryoPool;

    public UnshadedKryoShimService() {
        this(TinkerPopKryoRegistrator.class.getCanonicalName(), getDefaultKryoPoolSize());
    }

    public UnshadedKryoShimService(String sparkKryoRegistratorClassname, int kryoPoolSize) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.serializer", KryoSerializer.class.getCanonicalName());
        sparkConf.set("spark.kryo.registrator", sparkKryoRegistratorClassname);
        sparkKryoSerializer = new KryoSerializer(sparkConf);
        kryoPool = KryoPool.withByteArrayOutputStream(kryoPoolSize, new KryoInstantiator());
    }

    @Override
    public Object readClassAndObject(InputStream source) {
        SerDeState sds = null;
        try {
            sds = kryoPool.borrow();

            sds.setInput(source);

            return sds.readClassAndObject();
        } finally {
            kryoPool.release(sds);
        }
    }

    @Override
    public void writeClassAndObject(Object o, OutputStream sink) {
        SerDeState sds = null;
        try {
            sds = kryoPool.borrow();

            sds.writeClassAndObject(o); // this writes to an internal buffer

            sds.writeOutputTo(sink); // this copies the internal buffer to sink

            sink.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            kryoPool.release(sds);
        }
    }

    @Override
    public int getPriority() {
        return 1024;
    }

    private static int getDefaultKryoPoolSize() {
        String raw = System.getProperty(SPARK_KRYO_POOL_SIZE_SYSTEM_PROPERTY);

        int size = SPARK_KRYO_POOL_SIZE_DEFAULT;
        try {
            size = Integer.valueOf(raw);
            log.info("Setting kryo pool size to {} according to system property {}", size,
                    SPARK_KRYO_POOL_SIZE_SYSTEM_PROPERTY);
        } catch (NumberFormatException e) {
            log.error("System property {}={} could not be parsed as an integer, using default value {}",
                    SPARK_KRYO_POOL_SIZE_SYSTEM_PROPERTY, raw, size, e);
        }

        return size;
    }
}
