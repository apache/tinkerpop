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

import org.apache.tinkerpop.gremlin.jsr223.Customizer;

/**
 * Provides some custom compilation options to the {@link GremlinGroovyScriptEngine}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class CompilationOptionsCustomizer implements Customizer {

    private final long expectedCompilationTime;
    private final String cacheSpecification;

    private CompilationOptionsCustomizer(final Builder builder) {
        this.expectedCompilationTime = builder.expectedCompilationTime;
        this.cacheSpecification = builder.cacheSpecification;
    }

    public long getExpectedCompilationTime() {
        return expectedCompilationTime;
    }

    public String getClassMapCacheSpecification() {
        return cacheSpecification;
    }

    public static Builder build() {
        return new Builder();
    }

    public static class Builder {
        private long expectedCompilationTime;
        private String cacheSpecification = "softValues";

        public Builder setExpectedCompilationTime(final long expectedCompilationTime) {
            this.expectedCompilationTime = expectedCompilationTime;
            return this;
        }

        public Builder setClassMapCacheSpecification(final String cacheSpecification) {
            this.cacheSpecification = cacheSpecification;
            return this;
        }

        public CompilationOptionsCustomizer create() {
            return new CompilationOptionsCustomizer(this);
        }
    }
}
