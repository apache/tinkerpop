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
package org.apache.tinkerpop.gremlin.util.ser;

import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.Mapper;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Base {@link MessageSerializer} that serializers can implement to get some helper methods around configuring a
 * {@link Mapper.Builder}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractMessageSerializer<M> implements MessageSerializer<M> {
    public static final String TOKEN_IO_REGISTRIES = "ioRegistries";

    /**
     * Reads a list of fully qualified class names from the value of the {@link #TOKEN_IO_REGISTRIES} configuration
     * key. These classes should equate to {@link IoRegistry} implementations that will be assigned to the
     * {@link Mapper.Builder}.  The assumption is that the {@link IoRegistry} either has a static {@code instance()}
     * method or has a zero-arg constructor from which it can be instantiated.
     */
    protected void addIoRegistries(final Map<String, Object> config, final Mapper.Builder builder) {
        final List<String> classNameList = getListStringFromConfig(TOKEN_IO_REGISTRIES, config);

        classNameList.stream().forEach(className -> {
            try {
                final Class<?> clazz = Class.forName(className);
                try {
                    // try instance() first and then getInstance() which was deprecated in 3.2.4
                    final Method instanceMethod = tryInstanceMethod(clazz);
                    if (IoRegistry.class.isAssignableFrom(instanceMethod.getReturnType()))
                        builder.addRegistry((IoRegistry) instanceMethod.invoke(null));
                    else
                        throw new Exception();
                } catch (Exception methodex) {
                    // tried instance() and that failed so try newInstance() no-arg constructor
                    builder.addRegistry((IoRegistry) clazz.newInstance());
                }
            } catch (Exception ex) {
                throw new IllegalStateException(ex);
            }
        });
    }

    protected Method tryInstanceMethod(final Class clazz) {
        Method instanceMethod;
        try {
            instanceMethod = clazz.getDeclaredMethod("instance");
        } catch (Exception methodex) {
            instanceMethod = null;
        }

        if (null == instanceMethod) {
            try {
                instanceMethod = clazz.getDeclaredMethod("getInstance");
            } catch (Exception methodex) {
                instanceMethod = null;
            }
        }

        return instanceMethod;
    }

    /**
     * Gets a {@link List} of strings from the configuration object.
     */
    protected List<String> getListStringFromConfig(final String token, final Map<String, Object> config) {
        final List<String> classNameList;
        try {
            classNameList = (List<String>) config.getOrDefault(token, Collections.emptyList());
        } catch (Exception ex) {
            throw new IllegalStateException(String.format("Invalid configuration value of [%s] for [%s] setting on %s serialization configuration",
                    config.getOrDefault(token, ""), token, this.getClass().getName()), ex);
        }

        return classNameList;
    }
}