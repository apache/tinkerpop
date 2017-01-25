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
import org.apache.tinkerpop.gremlin.process.actors.ActorProgram;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.akka.process.actors.Constants.AKKA_ACTOR_SERIALIZATION_BINDINGS;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GryoSerializer implements Serializer {

    private final GryoPool gryoPool;

    public GryoSerializer(final ExtendedActorSystem actorSystem) {
        final Config config = actorSystem.settings().config();
        final Set<Class> gryoClasses = new HashSet<>();
        for (final Map.Entry<String, String> entry : ((Map<String, String>) config.getAnyRef(AKKA_ACTOR_SERIALIZATION_BINDINGS)).entrySet()) {
            if (entry.getValue().equals("gryo")) {
                gryoClasses.add(ClassUtil.getClassOrEnum(entry.getKey()));
            }
        }
        // remove Gryo 3.0 classes
        GryoVersion.V3_0.getRegistrations().forEach(type -> gryoClasses.remove(type.getTargetClass()));
        // this sucks. how to do this automatically?
        gryoClasses.remove(Short.class);
        gryoClasses.remove(Integer.class);
        gryoClasses.remove(Float.class);
        gryoClasses.remove(Double.class);
        gryoClasses.remove(Long.class);
        gryoClasses.remove(String.class);
        //
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
                        builder.referenceTracking(true). // config.getBoolean("gremlin.gryo.referenceTracking")).
                                registrationRequired(true). // config.getBoolean("gremlin.gryo.registrationRequired")).
                                version(GryoVersion.V3_0).
                                addCustom(gryoClasses.toArray(new Class[gryoClasses.size()])).
                                addRegistries(registryList)).create();
    }

    public static Map<String, String> getSerializerBindings(final ActorProgram<?> actorProgram, final Configuration configuration) {
        final Set<Class> programMessageClasses = new HashSet<>(actorProgram.getMessageTypes().keySet());
        programMessageClasses.add(DefaultActorsResult.class); // todo: may make this a Gryo3.0 class in the near future
        GryoVersion.V3_0.getRegistrations().forEach(type -> programMessageClasses.remove(type.getTargetClass()));
        final Map<String, String> bindings = new HashMap<>();
        GryoMapper.build().
                referenceTracking(configuration.getBoolean("gremlin.gryo.referenceTracking", true)).
                registrationRequired(configuration.getBoolean("gremlin.gryo.registrationRequired", true)).
                version(GryoVersion.V3_0).
                addCustom(programMessageClasses.toArray(new Class[programMessageClasses.size()])).
                addRegistries(IoRegistryHelper.createRegistries(configuration)).
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
