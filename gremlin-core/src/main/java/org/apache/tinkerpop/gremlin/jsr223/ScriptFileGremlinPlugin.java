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
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class ScriptFileGremlinPlugin extends AbstractGremlinPlugin {
    private static final String MODULE_NAME = "tinkerpop.script";

    public ScriptFileGremlinPlugin(final Builder builder) {
        super(MODULE_NAME, builder.appliesTo, new DefaultScriptCustomizer(builder.files));
    }

    public static Builder build() {
        return new Builder();
    }

    public static final class Builder {

        private final Set<String> appliesTo = new HashSet<>();
        private Set<File> files = new HashSet<>();

        private Builder() {}

        /**
         * The name of the {@link GremlinScriptEngine} that this module will apply to. Setting no values here will
         * make the module available to all the engines.
         */
        public Builder appliesTo(final Collection<String> scriptEngineName) {
            this.appliesTo.addAll(scriptEngineName);
            return this;
        }

        public Builder files(final Set<String> files) {
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
