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

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.util.Gremlin;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ImportGremlinModuleTest {

    @Test(expected = IllegalStateException.class)
    public void shouldImportSomething() {
        ImportGremlinModule.build().create();
    }

    @Test
    public void shouldImportClass() {
        final ImportGremlinModule module = ImportGremlinModule.build()
                .classImports(Collections.singletonList(Graph.class.getCanonicalName())).create();

        final ImportCustomizer customizer = (ImportCustomizer) module.getCustomizers().get()[0];
        assertEquals(1, module.getCustomizers().get().length);
        assertThat(customizer.getClassImports(), hasItems(Graph.class));
        assertEquals(1, customizer.getClassImports().size());
    }

    @Test
    public void shouldImportWildcardMethod() throws Exception {
        final Method zeroArgs = Gremlin.class.getMethod("version");
        final ImportGremlinModule module = ImportGremlinModule.build()
                .methodImports(Collections.singletonList(Gremlin.class.getCanonicalName() + "#*")).create();

        final ImportCustomizer customizer = (ImportCustomizer) module.getCustomizers().get()[0];
        assertEquals(1, module.getCustomizers().get().length);
        assertThat(customizer.getMethodImports(), hasItems(zeroArgs));

        // will also have the static main() method
        assertEquals(2, customizer.getMethodImports().size());
    }

    @Test
    public void shouldImportZeroArgMethod() throws Exception {
        final Method zeroArgs = Gremlin.class.getMethod("version");
        final ImportGremlinModule module = ImportGremlinModule.build()
                .methodImports(Collections.singletonList(toMethodDescriptor(zeroArgs))).create();

        final ImportCustomizer customizer = (ImportCustomizer) module.getCustomizers().get()[0];
        assertEquals(1, module.getCustomizers().get().length);
        assertThat(customizer.getMethodImports(), hasItems(zeroArgs));
        assertEquals(1, customizer.getMethodImports().size());
    }

    @Test
    public void shouldImportSingleArgMethod() throws Exception {
        final Method singleArg = IoCore.class.getMethod("createIoBuilder", String.class);
        final ImportGremlinModule module = ImportGremlinModule.build()
                .methodImports(Collections.singletonList(toMethodDescriptor(singleArg))).create();

        final ImportCustomizer customizer = (ImportCustomizer) module.getCustomizers().get()[0];
        assertEquals(1, module.getCustomizers().get().length);
        assertThat(customizer.getMethodImports(), hasItems(singleArg));
        assertEquals(1, customizer.getMethodImports().size());
    }

    @Test
    public void shouldThrowExceptionIfInvalidMethodDescriptor() throws Exception {
        final String badDescriptor = "Gremlin*version";
        try {
            ImportGremlinModule.build()
                    .methodImports(Collections.singletonList(badDescriptor)).create();
            fail("Should have failed parsing the method descriptor");
        } catch (IllegalArgumentException iae) {
            assertEquals(iae.getMessage(), "Could not read method descriptor - check format of: " + badDescriptor);
        }
    }

    @Test
    public void shouldImportWildcardEnum() throws Exception {
        final ImportGremlinModule module = ImportGremlinModule.build()
                .enumImports(Collections.singletonList(T.class.getCanonicalName() + "#*")).create();

        final ImportCustomizer customizer = (ImportCustomizer) module.getCustomizers().get()[0];
        assertEquals(1, module.getCustomizers().get().length);
        assertThat(customizer.getEnumImports(), hasItems(T.id, T.key, T.label, T.value));
        assertEquals(4, customizer.getEnumImports().size());
    }

    @Test
    public void shouldImportEnum() throws Exception {
        final ImportGremlinModule module = ImportGremlinModule.build()
                .enumImports(Collections.singletonList(T.class.getCanonicalName() + "#" + T.id.name())).create();

        final ImportCustomizer customizer = (ImportCustomizer) module.getCustomizers().get()[0];
        assertEquals(1, module.getCustomizers().get().length);
        assertThat(customizer.getEnumImports(), hasItems(T.id));
    }

    @Test
    public void shouldThrowExceptionIfInvalidEnumDescriptor() throws Exception {
        final String badDescriptor = "T*id";
        try {
            ImportGremlinModule.build()
                    .enumImports(Collections.singletonList(badDescriptor)).create();
            fail("Should have failed parsing the enum descriptor");
        } catch (IllegalArgumentException iae) {
            assertEquals("Could not read enum descriptor - check format of: " + badDescriptor, iae.getMessage());
        }
    }

    private static String toMethodDescriptor(final Method method) {
        return method.getDeclaringClass().getCanonicalName() +
                "#" +
                method.getName() +
                '(' +
                String.join(",", Stream.of(method.getParameters()).map(p -> p.getType().getCanonicalName()).collect(Collectors.toList())) +
                ')';
    }
}
