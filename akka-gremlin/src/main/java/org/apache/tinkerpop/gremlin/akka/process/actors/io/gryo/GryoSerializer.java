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

package org.apache.tinkerpop.gremlin.akka.process.actors.io.gryo;

import akka.actor.ExtendedActorSystem;
import akka.serialization.Serializer;
import com.typesafe.config.Config;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.actors.traversal.message.BarrierAddMessage;
import org.apache.tinkerpop.gremlin.process.actors.traversal.message.BarrierDoneMessage;
import org.apache.tinkerpop.gremlin.process.actors.traversal.message.SideEffectAddMessage;
import org.apache.tinkerpop.gremlin.process.actors.traversal.message.SideEffectSetMessage;
import org.apache.tinkerpop.gremlin.process.actors.traversal.message.StartMessage;
import org.apache.tinkerpop.gremlin.process.actors.traversal.message.Terminate;
import org.apache.tinkerpop.gremlin.process.actors.util.DefaultActorsResult;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoPool;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoVersion;
import org.apache.tinkerpop.gremlin.structure.io.util.IoRegistryHelper;
import org.apache.tinkerpop.gremlin.util.ClassUtil;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;
import scala.Option;

import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GryoSerializer implements Serializer {

    private final GryoPool gryoPool;

    public GryoSerializer(final ExtendedActorSystem actorSystem) {
        final Config config = actorSystem.settings().config();
        final List<IoRegistry> registryList;
        if (config.hasPath(IoRegistry.IO_REGISTRY)) {
            final Configuration configuration = new BaseConfiguration();
            configuration.setProperty(IoRegistry.IO_REGISTRY, config.getAnyRef(IoRegistry.IO_REGISTRY));
            registryList = IoRegistryHelper.createRegistries(configuration);
        } else {
            registryList = Collections.emptyList();
        }
        this.gryoPool = GryoPool.build().
                poolSize(10).
                initializeMapper(builder ->
                        builder.referenceTracking(true).
                                registrationRequired(true).
                                version(GryoVersion.V3_0).
                                addRegistries(registryList).
                                addCustom(
                                        Terminate.class,
                                        StartMessage.class,
                                        BarrierAddMessage.class,
                                        BarrierDoneMessage.class,
                                        SideEffectSetMessage.class,
                                        SideEffectAddMessage.class,
                                        DefaultActorsResult.class)).create();
    }

    public static Map<String, String> getSerializerBindings(final Configuration configuration) {
        final Map<String, String> bindings = new HashMap<>();
        GryoMapper.build().
                referenceTracking(true).
                registrationRequired(true).
                version(GryoVersion.V3_0).
                addRegistries(IoRegistryHelper.createRegistries(configuration)).
                addCustom(
                        Terminate.class,
                        StartMessage.class,
                        BarrierAddMessage.class,
                        BarrierDoneMessage.class,
                        SideEffectSetMessage.class,
                        SideEffectAddMessage.class,
                        DefaultActorsResult.class).
                create().
                getRegisteredClasses().
                stream().
                filter(clazz -> !clazz.isArray()). // be sure to make this right somehow
                forEach(clazz -> bindings.put(ClassUtil.getClassName(clazz), "gryo"));
        return bindings;
    }

    @Override
    public int identifier() {
        return GryoVersion.V3_0.hashCode();
    }

    @Override
    public boolean includeManifest() {
        return false;
    }

    @Override
    public byte[] toBinary(final Object object) {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final Output output = new Output(outputStream);
        this.gryoPool.writeWithKryo(kryo -> kryo.writeClassAndObject(output, object));
        output.flush();
        return outputStream.toByteArray();
    }

    @Override
    public Object fromBinary(final byte[] bytes) {
        final Input input = new Input(bytes);
        return this.gryoPool.readWithKryo(kryo -> kryo.readClassAndObject(input));
    }

    @Override
    public Object fromBinary(final byte[] bytes, final Class<?> aClass) {
        return fromBinary(bytes);
    }

    @Override
    public Object fromBinary(final byte[] bytes, final Option<Class<?>> option) {
        return option.isEmpty() ? this.fromBinary(bytes) : this.fromBinary(bytes, option.get());
    }
}
