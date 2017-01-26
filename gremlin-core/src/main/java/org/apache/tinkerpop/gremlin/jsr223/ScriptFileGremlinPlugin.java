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
package org.apache.tinkerpop.gremlin.jsr223;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Loads scripts from one or more files into the {@link GremlinScriptEngine} at startup. This {@link GremlinPlugin} is
 * not enabled for the {@code ServiceLoader}. It is designed to be instantiated manually. Those implementing
 * {@link GremlinScriptEngine} instances need to be concerned with accounting for this {@link Customizer}. It is
 * handled automatically by the {@link DefaultGremlinScriptEngineManager}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class ScriptFileGremlinPlugin extends AbstractGremlinPlugin {
    private static final String NAME = "tinkerpop.script";

    private ScriptFileGremlinPlugin(final Builder builder) {
        super(NAME, builder.appliesTo, new DefaultScriptCustomizer(builder.files));
    }

    public static Builder build() {
        return new Builder();
    }

    public static final class Builder {

        private final Set<String> appliesTo = new HashSet<>();
        private List<File> files = new ArrayList<>();

        private Builder() {}

        /**
         * The name of the {@link GremlinScriptEngine} that this module will apply to. Setting no values here will
         * make the module available to all the engines. Typically, this value should be set as a script's syntax will
         * be bound to the {@link GremlinScriptEngine} language.
         */
        public Builder appliesTo(final Collection<String> scriptEngineNames) {
            this.appliesTo.addAll(scriptEngineNames);
            return this;
        }

        public Builder files(final List<String> files) {
            for (String f : files) {
                final File file = new File(f);
                if (!file.exists()) throw new IllegalArgumentException(new FileNotFoundException(f));
                this.files.add(file);
            }

            return this;
        }

        public ScriptFileGremlinPlugin create() {
            return new ScriptFileGremlinPlugin(this);
        }
    }
}
