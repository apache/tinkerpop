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
package org.apache.tinkerpop.gremlin.groovy.jsr223;

import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.Customizer;
import org.apache.tinkerpop.gremlin.jsr223.GremlinPlugin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * A {@link GremlinPlugin} that provides access to low-level configuration options of the {@code GroovyScriptEngine}
 * itself.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GroovyCompilerGremlinPlugin extends AbstractGremlinPlugin {

    public enum Compilation {
        COMPILE_STATIC,
        TYPE_CHECKED,
        NONE
    }

    private static final String NAME = "tinkerpop.groovy";

    private GroovyCompilerGremlinPlugin(final Builder builder) {
        super(NAME, new HashSet<>(Collections.singletonList("gremlin-groovy")), builder.asCustomizers());
    }

    public static Builder build() {
        return new Builder();
    }

    public static final class Builder {

        private boolean interpreterMode;
        private boolean threadInterrupt;
        private long timeInMillis = 0;
        private Compilation compilation = Compilation.NONE;
        private String extensions = null;
        private int expectedCompilationTime = 5000;
        private String cacheSpec = "softValues";
        private boolean globalFunctionCacheEnabled = true;

        private Map<String,Object> keyValues = Collections.emptyMap();

        /**
         * If the time it takes to compile a script exceeds the specified time then a warning is written to the logs.
         * Defaults to 5000ms and must be greater than zero.
         */
        public Builder expectedCompilationTime(final int timeInMillis) {
            if (expectedCompilationTime <= 0) throw new IllegalArgumentException("expectedCompilationTime must be greater than zero");
            this.expectedCompilationTime = timeInMillis;
            return this;
        }

        public Builder enableInterpreterMode(final boolean interpreterMode) {
            this.interpreterMode = interpreterMode;
            return this;
        }

        public Builder compilerConfigurationOptions(final Map<String,Object> keyValues) {
            this.keyValues = keyValues;
            return this;
        }

        public Builder enableThreadInterrupt(final boolean threadInterrupt) {
            this.threadInterrupt = threadInterrupt;
            return this;
        }

        /**
         * Introduces timed checks to loops and other portions of a script to provide an interrupt for a long running
         * script. This configuration should not be used in conjunction with the Gremlin Server which has its own
         * {@code scriptEvaluationTimeout} which performs a similar task but in a more complete way specific to the
         * server. Configuring both may lead to inconsistent timeout errors returning from the server. This
         * configuration should only be used if configuring a standalone instance fo the {@link GremlinGroovyScriptEngine}.
         */
        public Builder timedInterrupt(final long timeInMillis) {
            this.timeInMillis = timeInMillis;
            return this;
        }

        public Builder compilation(final Compilation compilation) {
            this.compilation = compilation;
            return this;
        }

        public Builder compilation(final String compilation) {
            return compilation(Compilation.valueOf(compilation));
        }

        public Builder extensions(final String extensions) {
            this.extensions = extensions;
            return this;
        }

        /**
         * Sets the cache specification for the class map which holds compiled scripts and uses the comma separated
         * syntax of the caffeine cache for configuration.
         * <ul>
         *   <li>{@code initialCapacity=[integer]}: sets {@code Caffeine.initialCapacity}.
         *   <li>{@code maximumSize=[long]}: sets {@code Caffeine.maximumSize}.
         *   <li>{@code maximumWeight=[long]}: sets {@code Caffeine.maximumWeight}.
         *   <li>{@code expireAfterAccess=[duration]}: sets {@code Caffeine.expireAfterAccess}.
         *   <li>{@code expireAfterWrite=[duration]}: sets {@code Caffeine.expireAfterWrite}.
         *   <li>{@code refreshAfterWrite=[duration]}: sets {@code Caffeine.refreshAfterWrite}.
         *   <li>{@code weakKeys}: sets {@code Caffeine.weakKeys}.
         *   <li>{@code weakValues}: sets {@code Caffeine.weakValues}.
         *   <li>{@code softValues}: sets {@code Caffeine.softValues}.
         *   <li>{@code recordStats}: sets {@code Caffeine.recordStats}.
         * </ul>
         * Durations are represented by an integer, followed by one of "d", "h", "m", or "s", representing
         * days, hours, minutes, or seconds respectively. Whitespace before and after commas and equal signs is
         * ignored. Keys may not be repeated; it is also illegal to use the following pairs of keys in a single value:
         * <ul>
         *   <li>{@code maximumSize} and {@code maximumWeight}
         *   <li>{@code weakValues} and {@code softValues}
         * </ul>
         */
        public Builder classMapCacheSpecification(final String cacheSpec) {
            this.cacheSpec = cacheSpec;
            return this;
        }

        /**
         * Determines if the global function cache in the script engine is enabled or not. It is enabled by default.
         */
        public Builder globalFunctionCacheEnabled(final boolean enabled) {
            this.globalFunctionCacheEnabled = enabled;
            return this;
        }

        Customizer[] asCustomizers() {
            final List<Customizer> list = new ArrayList<>();

            if (interpreterMode)
                list.add(new InterpreterModeGroovyCustomizer());

            if (!keyValues.isEmpty())
                list.add(new ConfigurationGroovyCustomizer(keyValues));

            if (threadInterrupt)
                list.add(new ThreadInterruptGroovyCustomizer());

            if (timeInMillis > 0)
                list.add(new TimedInterruptGroovyCustomizer(timeInMillis));

            list.add(CompilationOptionsCustomizer.build().
                    enableGlobalFunctionCache(globalFunctionCacheEnabled).
                    setExpectedCompilationTime(expectedCompilationTime > 0 ? expectedCompilationTime : 5000).
                    setClassMapCacheSpecification(cacheSpec).create());

            if (compilation == Compilation.COMPILE_STATIC)
                list.add(new CompileStaticGroovyCustomizer(extensions));
            else if (compilation == Compilation.TYPE_CHECKED)
                list.add(new TypeCheckedGroovyCustomizer(extensions));
            else if (compilation != Compilation.NONE)
                throw new IllegalStateException("Use of unknown compilation type: " + compilation);

            final Customizer[] customizers = new Customizer[list.size()];
            list.toArray(customizers);
            return customizers;
        }

        public GroovyCompilerGremlinPlugin create() {
            return new GroovyCompilerGremlinPlugin(this);
        }
    }
}
