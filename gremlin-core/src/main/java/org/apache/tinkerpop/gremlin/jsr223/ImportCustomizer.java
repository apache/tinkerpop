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

import java.lang.reflect.Method;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Provides the list of imports to apply to a {@link GremlinScriptEngine} instance.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface ImportCustomizer extends Customizer {
    /**
     * @deprecated As of release 3.2.4, not replaced.
     */
    @Deprecated
    public static final ImportCustomizer GREMLIN_CORE = DefaultImportCustomizer.build()
            .addClassImports(CoreImports.getClassImports())
            .addEnumImports(CoreImports.getEnumImports())
            .addMethodImports(CoreImports.getMethodImports()).create();

    /**
     * Gets the set of classes to be imported to the {@link GremlinScriptEngine}.
     */
    public Set<Class> getClassImports();

    /**
     * Gets the set of static methods to be imported to the {@link GremlinScriptEngine}.
     */
    public Set<Method> getMethodImports();

    /**
     * Gets the set of enums to be imported to the {@link GremlinScriptEngine}.
     */
    public Set<Enum> getEnumImports();

    /**
     * Gets the set of packages from the {@link #getClassImports()}.
     */
    public default Set<Package> getClassPackages() {
        return getClassImports().stream().map(Class::getPackage).collect(Collectors.toSet());
    }

    /**
     * Gets the set of classes from the {@link #getMethodImports()}.
     */
    public default Set<Class> getMethodClasses() {
        return getMethodImports().stream().map(Method::getDeclaringClass).collect(Collectors.toSet());
    }

    /**
     * Gets the set of classes from the {@link #getEnumImports()}.
     */
    public default Set<Class> getEnumClasses() {
        return getEnumImports().stream().map(Enum::getDeclaringClass).collect(Collectors.toSet());
    }
}
