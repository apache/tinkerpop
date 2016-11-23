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

import org.apache.tinkerpop.gremlin.util.CoreImports;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Simple implementation of the {@link ImportCustomizer} which allows direct setting of all the different import types.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DefaultImportCustomizer implements ImportCustomizer {

    private final Set<Class> classImports;
    private final Set<Method> methodImports;
    private final Set<Enum> enumImports;

    private DefaultImportCustomizer(final Builder builder) {
        classImports = builder.classImports;
        methodImports = builder.methodImports;
        enumImports = builder.enumImports;
    }

    public Set<Class> getClassImports() {
        return Collections.unmodifiableSet(classImports);
    }

    public Set<Method> getMethodImports() {
        return Collections.unmodifiableSet(methodImports);
    }

    public Set<Enum> getEnumImports() {
        return Collections.unmodifiableSet(enumImports);
    }

    public static Builder build() {
        return new Builder();
    }

    public static class Builder {
        private Set<Class> classImports = new HashSet<>();
        private Set<Method> methodImports = new HashSet<>();
        private Set<Enum> enumImports = new HashSet<>();

        private Builder() {}

        public Builder addClassImports(final Class... clazz) {
            classImports.addAll(Arrays.asList(clazz));
            return this;
        }

        public Builder addClassImports(final Collection<Class> classes) {
            classImports.addAll(classes);
            return this;
        }

        public Builder addMethodImports(final Method... method) {
            methodImports.addAll(Arrays.asList(method));
            return this;
        }

        public Builder addMethodImports(final Collection<Method> methods) {
            methodImports.addAll(methods);
            return this;
        }

        public Builder addEnumImports(final Enum... e) {
            enumImports.addAll(Arrays.asList(e));
            return this;
        }

        public Builder addEnumImports(final Collection<Enum> enums) {
            enumImports.addAll(enums);
            return this;
        }

        public DefaultImportCustomizer create() {
            return new DefaultImportCustomizer(this);
        }
    }
}
