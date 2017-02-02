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

import groovy.lang.GroovyClassLoader;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.codehaus.groovy.util.ReferenceBundle;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * A {@code GroovyClassLoader} extension that uses a {@link ManagedConcurrentValueMap} configured with soft references.
 * In this way, the classloader will "forget" scripts allowing them to be garbage collected and thus prevent memory
 * issues in JVM metaspace.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class GremlinGroovyClassLoader extends GroovyClassLoader {
    private final ManagedConcurrentValueMap<String, Class> classSoftCache;

    public GremlinGroovyClassLoader(final ClassLoader parent, final CompilerConfiguration conf) {
        super(parent, conf);
        classSoftCache = new ManagedConcurrentValueMap<>(ReferenceBundle.getSoftBundle());
    }

    @Override
    protected void removeClassCacheEntry(final String name) {
        this.classSoftCache.remove(name);
    }

    @Override
    protected void setClassCacheEntry(final Class cls) {
        this.classSoftCache.put(cls.getName(), cls);
    }

    @Override
    protected Class getClassCacheEntry(final String name) {
        if(null == name)  return null;
        return this.classSoftCache.get(name);
    }
}
