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
package org.apache.tinkerpop.gremlin.groovy;

import org.apache.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DefaultDefaultImportCustomizerProviderTest {
    static {
        SugarLoader.load();
    }

    @Test
    public void shouldReturnDefaultImports() {
        final DefaultImportCustomizerProvider provider = new DefaultImportCustomizerProvider();
        assertImportsInProvider(provider);
    }

    @Test
    public void shouldReturnWithExtraStaticImports() {
        final Set<String> statics = new HashSet<>();
        statics.add("com.test.This.*");
        statics.add("com.test.That.OTHER");
        final DefaultImportCustomizerProvider provider = new DefaultImportCustomizerProvider(new HashSet<>(), statics);
        assertImportsInProvider(provider);
    }

    @Test
    public void shouldReturnWithExtraImports() {
        final Set<String> imports = new HashSet<>();
        imports.add("com.test.that.*");
        imports.add("com.test.that.That");
        final DefaultImportCustomizerProvider provider = new DefaultImportCustomizerProvider(imports, new HashSet<>());
        assertImportsInProvider(provider);
    }

    @Test
    public void shouldReturnWithBothImportTypes() {
        final Set<String> imports = new HashSet<>();
        imports.add("com.test.that.*");
        imports.add("com.test.that.That");

        final Set<String> statics = new HashSet<>();
        statics.add("com.test.This.*");
        statics.add("com.test.That.OTHER");

        final DefaultImportCustomizerProvider provider = new DefaultImportCustomizerProvider(imports, statics);
        assertImportsInProvider(provider);
    }

    private static void assertImportsInProvider(DefaultImportCustomizerProvider provider) {
        final Set<String> allImports = provider.getAllImports();

        final Set<String> imports = provider.getImports();
        final Set<String> staticImports = provider.getStaticImports();
        final Set<String> extraImports = provider.getExtraImports();
        final Set<String> extraStaticImports = provider.getExtraStaticImports();

        assertEquals(imports.size() + staticImports.size() + extraImports.size() + extraStaticImports.size(), allImports.size());
        assertTrue(allImports.containsAll(imports));
        assertTrue(allImports.containsAll(staticImports));
        assertTrue(allImports.containsAll(extraImports));
        assertTrue(allImports.containsAll(extraStaticImports));
    }
}
