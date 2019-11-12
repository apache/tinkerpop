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


import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.python.PythonBroadcast;
import org.apache.spark.broadcast.TorrentBroadcast;
import org.apache.spark.network.util.ByteUnit;
import org.apache.spark.scheduler.CompressedMapStatus;
import org.apache.spark.scheduler.HighlyCompressedMapStatus;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.storage.BlockManagerId;
import org.apache.spark.util.SerializableConfiguration;
import org.apache.spark.util.collection.CompactBuffer;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.HadoopPools;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.ObjectWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.spark.process.computer.payload.MessagePayload;
import org.apache.tinkerpop.gremlin.spark.process.computer.payload.ViewIncomingPayload;
import org.apache.tinkerpop.gremlin.spark.process.computer.payload.ViewOutgoingPayload;
import org.apache.tinkerpop.gremlin.spark.process.computer.payload.ViewPayload;
import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoPool;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoVersion;
import org.apache.tinkerpop.shaded.kryo.io.Output;
import org.apache.tinkerpop.shaded.kryo.serializers.ExternalizableSerializer;
import org.apache.tinkerpop.shaded.kryo.serializers.JavaSerializer;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.mutable.WrappedArray;
import scala.runtime.BoxedUnit;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GryoSerializer extends Serializer implements Serializable {

    //private final Option<String> userRegistrator;
    private final int bufferSize;
    private final int maxBufferSize;
    private final boolean referenceTracking;
    private final boolean registrationRequired;

    public GryoSerializer(final SparkConf sparkConfiguration) {
        final long bufferSizeKb = sparkConfiguration.getSizeAsKb("spark.kryoserializer.buffer", "64k");
        final long maxBufferSizeMb = sparkConfiguration.getSizeAsMb("spark.kryoserializer.buffer.max", "64m");
        this.referenceTracking = sparkConfiguration.getBoolean("spark.kryo.referenceTracking", true);
        this.registrationRequired = sparkConfiguration.getBoolean(Constants.SPARK_KRYO_REGISTRATION_REQUIRED, false);
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
        // create a GryoPool and store it in static HadoopPools
        final List<Object> ioRegistries = new ArrayList<>();
        ioRegistries.addAll(makeApacheConfiguration(sparkConfiguration).getList(IoRegistry.IO_REGISTRY, Collections.emptyList()));
        ioRegistries.add(SparkIoRegistry.class.getCanonicalName().replace("." + SparkIoRegistry.class.getSimpleName(), "$" + SparkIoRegistry.class.getSimpleName()));
        HadoopPools.initialize(GryoPool.build().
                version(GryoVersion.valueOf(sparkConfiguration.get(GryoPool.CONFIG_IO_GRYO_VERSION, GryoPool.CONFIG_IO_GRYO_POOL_VERSION_DEFAULT.name()))).
                poolSize(sparkConfiguration.getInt(GryoPool.CONFIG_IO_GRYO_POOL_SIZE, GryoPool.CONFIG_IO_GRYO_POOL_SIZE_DEFAULT)).
                ioRegistries(ioRegistries).
                initializeMapper(builder ->
                        builder.referenceTracking(this.referenceTracking).
                                registrationRequired(this.registrationRequired)).
                create());
    }

    public Output newOutput() {
        return new Output(this.bufferSize, this.maxBufferSize);
    }

    public GryoPool getGryoPool() {
        return HadoopPools.getGryoPool();
    }

    @Override
    public SerializerInstance newInstance() {
        return new GryoSerializerInstance(this);
    }

    private static Configuration makeApacheConfiguration(final SparkConf sparkConfiguration) {
        final BaseConfiguration apacheConfiguration = new BaseConfiguration();
        for (final Tuple2<String, String> tuple : sparkConfiguration.getAll()) {
            apacheConfiguration.setProperty(tuple._1(), tuple._2());
        }
        return apacheConfiguration;
    }

    public static class SparkIoRegistry extends AbstractIoRegistry {
        private static final SparkIoRegistry INSTANCE = new SparkIoRegistry();

        private SparkIoRegistry() {
            try {
                super.register(GryoIo.class, Tuple2.class, new Tuple2Serializer());
                super.register(GryoIo.class, Tuple2[].class, null);
                super.register(GryoIo.class, Tuple3.class, new Tuple3Serializer());
                super.register(GryoIo.class, Tuple3[].class, null);
                super.register(GryoIo.class, CompactBuffer.class, new CompactBufferSerializer());
                super.register(GryoIo.class, CompactBuffer[].class, null);
                super.register(GryoIo.class, CompressedMapStatus.class, null);
                super.register(GryoIo.class, BlockManagerId.class, null);
                super.register(GryoIo.class, HighlyCompressedMapStatus.class, new ExternalizableSerializer());  // externalizable implemented so its okay
                super.register(GryoIo.class, TorrentBroadcast.class, null);
                super.register(GryoIo.class, PythonBroadcast.class, null);
                super.register(GryoIo.class, BoxedUnit.class, null);
                super.register(GryoIo.class, Class.forName("scala.reflect.ManifestFactory$AnyManifest"), new JavaSerializer());
                super.register(GryoIo.class, Class.forName("scala.reflect.ClassTag$GenericClassTag"), new JavaSerializer());
                super.register(GryoIo.class, Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"), new JavaSerializer());
                super.register(GryoIo.class, Class.forName("org.apache.spark.internal.io.FileCommitProtocol$EmptyTaskCommitMessage$"), new JavaSerializer());
                super.register(GryoIo.class, Class.forName("scala.collection.immutable.Map$EmptyMap$"), new JavaSerializer());
                super.register(GryoIo.class, Class.forName("scala.collection.immutable.Map"), new JavaSerializer());
                super.register(GryoIo.class, Class.forName("scala.None$"), new JavaSerializer());
                super.register(GryoIo.class, Class.forName("scala.Some$"), new JavaSerializer());
                super.register(GryoIo.class, Class.forName("scala.Some"), new JavaSerializer());
                super.register(GryoIo.class, WrappedArray.ofRef.class, new WrappedArraySerializer());
                super.register(GryoIo.class, MessagePayload.class, null);
                super.register(GryoIo.class, ViewIncomingPayload.class, null);
                super.register(GryoIo.class, ViewOutgoingPayload.class, null);
                super.register(GryoIo.class, ViewPayload.class, null);
                super.register(GryoIo.class, SerializableConfiguration.class, new JavaSerializer());
                super.register(GryoIo.class, VertexWritable.class, new VertexWritableSerializer());
                super.register(GryoIo.class, ObjectWritable.class, new ObjectWritableSerializer());
            } catch (final ClassNotFoundException e) {
                throw new IllegalStateException(e);
            }
        }

        public static SparkIoRegistry instance() {
            return INSTANCE;
        }
    }
}