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

/**
 * A {@code GroovyClassLoader} extension that provides access to the {@code removeClassCacheEntry(String)} method.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class GremlinGroovyClassLoader extends GroovyClassLoader {
    public GremlinGroovyClassLoader(final ClassLoader parent, final CompilerConfiguration conf) {
        super(parent, conf);
    }

    @Override
    protected void removeClassCacheEntry(final String name) {
        super.removeClassCacheEntry(name);
    }
}
