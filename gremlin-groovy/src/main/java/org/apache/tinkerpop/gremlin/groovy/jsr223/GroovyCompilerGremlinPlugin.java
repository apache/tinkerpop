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

        private Map<String,Object> keyValues = Collections.emptyMap();

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

            if (compilation == Compilation.COMPILE_STATIC)
                list.add(new CompileStaticGroovyCustomizer(extensions));
            else if (compilation == Compilation.TYPE_CHECKED)
                list.add(new TypeCheckedGroovyCustomizer(extensions));
            else if (compilation != Compilation.NONE)
                throw new IllegalStateException("Use of unknown compilation type: " + compilation);

            if (list.isEmpty()) throw new IllegalStateException("No customizer options have been selected for this plugin");

            final Customizer[] customizers = new Customizer[list.size()];
            list.toArray(customizers);
            return customizers;
        }

        public GroovyCompilerGremlinPlugin create() {
            return new GroovyCompilerGremlinPlugin(this);
        }
    }
}
