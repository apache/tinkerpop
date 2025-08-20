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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.CaffeineSpec;

import java.util.Collections;
import java.util.Set;

/**
 * Exposes {@link Customizer} implementations for configuring the {@link GremlinLangScriptEngine}.
 */
public class GremlinLangPlugin extends AbstractGremlinPlugin {
    private static final String NAME = "tinkerpop.gremlin-language";
    private static final GremlinLangScriptEngineFactory factory = new GremlinLangScriptEngineFactory();
    private static final Set<String> APPLIES_TO = Collections.singleton(factory.getEngineName());

    private GremlinLangPlugin(final Builder builder) {
        super(NAME, APPLIES_TO,
                new GremlinLangCustomizer(builder.cacheEnabled,
                        builder.caffeineSpec != null ? Caffeine.from(builder.caffeineSpec) : Caffeine.newBuilder()));
    }

    /**
     * Builds a set of static bindings.
     */
    public static GremlinLangPlugin.Builder build() {
        return new GremlinLangPlugin.Builder();
    }

    public static class Builder {
        private boolean cacheEnabled = false;
        private CaffeineSpec caffeineSpec = null;

        public Builder cacheEnabled(final boolean cacheEnabled) {
            this.cacheEnabled = cacheEnabled;
            return this;
        }

        /**
         * Provide a Caffeine spec formatted cache definition to configure the cache. For example,
         * "maximumWeight=1000, expireAfterWrite=10m", see
         * <a href="https://github.com/ben-manes/caffeine/wiki/Specification">Caffeine Documatation</a> for details.
         */
        public Builder caffeine(final String spec) {
            this.caffeineSpec = CaffeineSpec.parse(spec);
            return this;
        }

        public GremlinLangPlugin create() {
            return new GremlinLangPlugin(this);
        }
    }
}
