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
package org.apache.tinkerpop.gremlin.process.traversal.dsl;

import org.junit.Test;

import java.io.InputStream;
import java.lang.module.ModuleDescriptor;
import java.lang.module.ModuleFinder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ModuleDescriptorVerificationTest {

    @Test
    public void shouldHaveExpectedDescriptor() throws Exception {
        final Path jar = Paths.get(System.getProperty("gremlin.jpms.artifactJar"));
        final ModuleDescriptor descriptor = ModuleFinder.of(jar).findAll().iterator().next().descriptor();
        assertFalse(descriptor.isAutomatic());
        assertEquals("org.apache.tinkerpop.gremlin.annotations", descriptor.name());
        assertEquals(Collections.singleton("org.apache.tinkerpop.gremlin.process.traversal.dsl"),
                descriptor.exports().stream().map(ModuleDescriptor.Exports::source).collect(Collectors.toSet()));
        assertTrue(descriptor.exports().stream().noneMatch(ModuleDescriptor.Exports::isQualified));
        assertTrue(descriptor.opens().isEmpty());
        assertFalse(descriptor.isOpen());
        assertTrue(descriptor.uses().isEmpty());

        assertEquals(1, descriptor.provides().size());
        final ModuleDescriptor.Provides provides = descriptor.provides().iterator().next();
        assertEquals("javax.annotation.processing.Processor", provides.service());
        assertEquals(Collections.singletonList(
                "org.apache.tinkerpop.gremlin.process.traversal.dsl.GremlinDslProcessor"), provides.providers());
        try (JarFile jarFile = new JarFile(jar.toFile());
             InputStream in = jarFile.getInputStream(
                     jarFile.getJarEntry("META-INF/services/javax.annotation.processing.Processor"))) {
            final Set<String> registered = Arrays.stream(new String(in.readAllBytes(), StandardCharsets.UTF_8).
                    split("\\R")).map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toSet());
            assertEquals(new java.util.HashSet<>(provides.providers()), registered);
        }
    }
}
