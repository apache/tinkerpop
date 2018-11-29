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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
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
    private final Set<Field> fieldImports;

    private DefaultImportCustomizer(final Builder builder) {
        classImports = builder.classImports;
        methodImports = builder.methodImports;
        enumImports = builder.enumImports;
        fieldImports = builder.fieldImports;
    }

    @Override
    public Set<Class> getClassImports() {
        return Collections.unmodifiableSet(classImports);
    }

    @Override
    public Set<Method> getMethodImports() {
        return Collections.unmodifiableSet(methodImports);
    }

    @Override
    public Set<Enum> getEnumImports() {
        return Collections.unmodifiableSet(enumImports);
    }

    @Override
    public Set<Field> getFieldImports() {
        return Collections.unmodifiableSet(fieldImports);
    }

    public static Builder build() {
        return new Builder();
    }

    public static class Builder {
        private Set<Class> classImports = new LinkedHashSet<>();
        private Set<Method> methodImports = new LinkedHashSet<>();
        private Set<Enum> enumImports = new LinkedHashSet<>();
        private Set<Field> fieldImports = new LinkedHashSet<>();

        private Builder() {}

        /**
         * Adds classes that will be imported to the {@code ScriptEngine}.
         */
        public Builder addClassImports(final Class... clazz) {
            classImports.addAll(Arrays.asList(clazz));
            return this;
        }

        /**
         * Overload to {@link #addClassImports(Class[])}.
         */
        public Builder addClassImports(final Collection<Class> classes) {
            classImports.addAll(classes);
            return this;
        }

        /**
         * Adds methods that are meant to be imported statically to the engine. When adding methods be sure that
         * the classes of those methods are added to the {@link #addClassImports(Class[])} or
         * {@link #addClassImports(Collection)}. If they are not added then the certain {@code ScriptEngine} instances
         * may have problems importing the methods (e.g. gremlin-python).
         */
        public Builder addMethodImports(final Method... method) {
            methodImports.addAll(Arrays.asList(method));
            return this;
        }

        /**
         * Overload to {@link #addMethodImports(Method...)}.
         */
        public Builder addMethodImports(final Collection<Method> methods) {
            methodImports.addAll(methods);
            return this;
        }

        /**
         * Adds fields that are meant to be imported statically to the engine. When adding fields be sure that
         * the classes of those fields are added to the {@link #addClassImports(Class[])} or
         * {@link #addClassImports(Collection)}. If they are not added then the certain {@code ScriptEngine} instances
         * may have problems importing the methods (e.g. gremlin-python).
         */
        public Builder addFieldImports(final Field... field) {
            fieldImports.addAll(Arrays.asList(field));
            return this;
        }

        /**
         * Overload to {@link #addFieldImports(Field...)}.
         */
        public Builder addFieldImports(final Collection<Field> fields) {
            fieldImports.addAll(fields);
            return this;
        }

        /**
         * Adds methods that are meant to be imported statically to the engine. When adding methods be sure that
         * the classes of those methods are added to the {@link #addClassImports(Class[])} or
         * {@link #addClassImports(Collection)}. If they are not added then the certain {@code ScriptEngine} instances
         * may have problems importing the methods (e.g. gremlin-python).
         */
        public Builder addEnumImports(final Enum... e) {
            enumImports.addAll(Arrays.asList(e));
            return this;
        }

        /**
         * Overload to {@link #addEnumImports(Enum[])}.
         */
        public Builder addEnumImports(final Collection<Enum> enums) {
            enumImports.addAll(enums);
            return this;
        }

        public DefaultImportCustomizer create() {
            return new DefaultImportCustomizer(this);
        }
    }
}
