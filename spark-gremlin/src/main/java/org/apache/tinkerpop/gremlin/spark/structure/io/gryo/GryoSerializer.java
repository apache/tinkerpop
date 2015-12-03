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


import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.spark.SerializableWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.python.PythonBroadcast;
import org.apache.spark.broadcast.HttpBroadcast;
import org.apache.spark.network.util.ByteUnit;
import org.apache.spark.scheduler.CompressedMapStatus;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.util.SerializableConfiguration;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.ObjectWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.spark.process.computer.payload.MessagePayload;
import org.apache.tinkerpop.gremlin.spark.process.computer.payload.ViewIncomingPayload;
import org.apache.tinkerpop.gremlin.spark.process.computer.payload.ViewOutgoingPayload;
import org.apache.tinkerpop.gremlin.spark.process.computer.payload.ViewPayload;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoPool;
import org.apache.tinkerpop.shaded.kryo.io.Output;
import org.apache.tinkerpop.shaded.kryo.serializers.JavaSerializer;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.mutable.WrappedArray;
import scala.runtime.BoxedUnit;

import java.util.Collections;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GryoSerializer extends Serializer {

    //private final Option<String> userRegistrator;
    private final int bufferSize;
    private final int maxBufferSize;

    private final GryoPool gryoPool;

    public GryoSerializer(final SparkConf sparkConfiguration) {
        final long bufferSizeKb = sparkConfiguration.getSizeAsKb("spark.kryoserializer.buffer", "64k");
        final long maxBufferSizeMb = sparkConfiguration.getSizeAsMb("spark.kryoserializer.buffer.max", "64m");
        final boolean referenceTracking = sparkConfiguration.getBoolean("spark.kryo.referenceTracking", true);
        final boolean registrationRequired = sparkConfiguration.getBoolean("spark.kryo.registrationRequired", false);
        if (bufferSizeKb >= ByteUnit.GiB.toKiB(2L)) {
            throw new IllegalArgumentException("spark.kryoserializer.buffer must be less than 2048 mb, got: " + bufferSizeKb + " mb.");
        } else {
            this.bufferSize = (int) ByteUnit.KiB.toBytes(bufferSizeKb);
            if (maxBufferSizeMb >= ByteUnit.GiB.toMiB(2L)) {
                throw new IllegalArgumentException("spark.kryoserializer.buffer.max must be less than 2048 mb, got: " + maxBufferSizeMb + " mb.");
            } else {
                this.maxBufferSize = (int) ByteUnit.MiB.toBytes(maxBufferSizeMb);
                //this.userRegistrator = sparkConfiguration.getOption("spark.kryo.registrator");
            }
        }
        this.gryoPool = GryoPool.build().
                poolSize(sparkConfiguration.getInt(GryoPool.CONFIG_IO_GRYO_POOL_SIZE, 256)).
                ioRegistries(makeApacheConfiguration(sparkConfiguration).getList(GryoPool.CONFIG_IO_REGISTRY, Collections.emptyList())).
                initializeMapper(builder -> {
                    try {
                        builder.addCustom(SerializableWritable.class, new JavaSerializer())
                                .addCustom(Tuple2.class, new JavaSerializer())
                                .addCustom(Tuple2[].class, new JavaSerializer())
                                .addCustom(Tuple3.class, new JavaSerializer())
                                .addCustom(Tuple3[].class, new JavaSerializer())
                                .addCustom(CompressedMapStatus.class, new JavaSerializer())
                                .addCustom(HttpBroadcast.class, new JavaSerializer())
                                .addCustom(PythonBroadcast.class, new JavaSerializer())
                                .addCustom(BoxedUnit.class, new JavaSerializer())
                                .addCustom(Class.forName("scala.reflect.ClassTag$$anon$1"), new JavaSerializer())
                                .addCustom(WrappedArray.ofRef.class, new WrappedArraySerializer())
                                .addCustom(MessagePayload.class)
                                .addCustom(ViewIncomingPayload.class)
                                .addCustom(ViewOutgoingPayload.class)
                                .addCustom(ViewPayload.class)
                                .addCustom(SerializableConfiguration.class, new JavaSerializer())
                                .addCustom(VertexWritable.class, new JavaSerializer())
                                .addCustom(ObjectWritable.class, new JavaSerializer())
                                .referenceTracking(referenceTracking)
                                .registrationRequired(registrationRequired);
                        // add these as we find ClassNotFoundExceptions
                    } catch (final ClassNotFoundException e) {
                        throw new IllegalStateException(e);
                    }
                }).create();
    }

    public Output newOutput() {
        return new Output(this.bufferSize, this.maxBufferSize);
    }

    public GryoPool getGryoPool() {
        return this.gryoPool;
    }

    @Override
    public SerializerInstance newInstance() {
        return new GryoSerializerInstance(this);
    }

    private static Configuration makeApacheConfiguration(final SparkConf sparkConfiguration) {
        final BaseConfiguration apacheConfiguration = new BaseConfiguration();
        apacheConfiguration.setDelimiterParsingDisabled(true);
        for (final Tuple2<String, String> tuple : sparkConfiguration.getAll()) {
            apacheConfiguration.setProperty(tuple._1(), tuple._2());
        }
        return apacheConfiguration;
    }
}