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
package org.apache.tinkerpop.gremlin.driver.ser;

import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.Mapper;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Base {@link MessageSerializer} that serializers can implement to get some helper methods around configuring a
 * {@link org.apache.tinkerpop.gremlin.structure.io.Mapper.Builder}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractMessageSerializer implements MessageSerializer {
    public static final String TOKEN_IO_REGISTRIES = "ioRegistries";

    /**
     * Reads a list of fully qualified class names from the value of the {@link #TOKEN_IO_REGISTRIES} configuration
     * key. These classes should equate to {@link IoRegistry} implementations that will be assigned to the
     * {@link org.apache.tinkerpop.gremlin.structure.io.Mapper.Builder}.  The assumption is that the
     * {@link IoRegistry} either has a static {@code getInstance()} method or has a zero-arg constructor from which
     * it can be instantiated.
     */
    protected void addIoRegistries(final Map<String, Object> config, final Mapper.Builder builder) {
        final List<String> classNameList = getListStringFromConfig(TOKEN_IO_REGISTRIES, config);

        classNameList.stream().forEach(className -> {
            try {
                final Class<?> clazz = Class.forName(className);
                try {
                    final Method instanceMethod = clazz.getDeclaredMethod("getInstance");
                    if (IoRegistry.class.isAssignableFrom(instanceMethod.getReturnType()))
                        builder.addRegistry((IoRegistry) instanceMethod.invoke(null));
                    else
                        throw new Exception();
                } catch (Exception methodex) {
                    // tried getInstance() and that failed so try newInstance() no-arg constructor
                    builder.addRegistry((IoRegistry) clazz.newInstance());
                }
            } catch (Exception ex) {
                throw new IllegalStateException(ex);
            }
        });
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