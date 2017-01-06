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
package org.apache.tinkerpop.gremlin.groovy.jsr223.customizer;

import org.apache.tinkerpop.gremlin.groovy.CompilerCustomizerProvider;
import org.apache.tinkerpop.gremlin.jsr223.Customizer;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.codehaus.groovy.control.customizers.CompilationCustomizer;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Allows configurations to be directly supplied to a groovy {@code CompilerConfiguration} when a
 * {@link org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine} is initialized, providing fine-grained
 * control over its internals.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @deprecated As of release 3.2.4, not replaced by a public class.
 */
@Deprecated
public class ConfigurationCustomizerProvider implements CompilerCustomizerProvider {

    private final Map<String,Object> properties;

    /**
     * Creates a new instance using configuration values specified
     */
    public ConfigurationCustomizerProvider(final Object... keyValues) {
        if (null == keyValues || keyValues.length == 0)
            throw new IllegalArgumentException("ConfigurationCustomizerProvider must have key/values specified");

        if (keyValues.length % 2 != 0)
            throw new IllegalArgumentException("The keyValues must have an even number of values");

        properties = ElementHelper.asMap(keyValues);
    }

    /**
     * Creates a new instance using configuration values specified
     */
    public ConfigurationCustomizerProvider(final Map<String,Object> keyValues) {
        properties = keyValues;
    }

    public CompilerConfiguration applyCustomization(final CompilerConfiguration compilerConfiguration) {
        final Class<CompilerConfiguration> clazz = CompilerConfiguration.class;
        final List<Method> methods = Arrays.asList(clazz.getMethods());
        for (Map.Entry<String,Object> entry : properties.entrySet()) {
            final Method method = methods.stream().filter(m -> m.getName().equals("set" + entry.getKey())).findFirst()
                   .orElseThrow(() -> new IllegalStateException("Invalid setting [" + entry.getKey() + "] for CompilerConfiguration"));

            try {
                method.invoke(compilerConfiguration, entry.getValue());
            } catch (Exception ex) {
                throw new IllegalStateException(ex);
            }
        }

        return compilerConfiguration;
    }

    @Override
    public CompilationCustomizer create() {
        throw new UnsupportedOperationException("This is a marker implementation that does not create a CompilationCustomizer instance");
    }
}
