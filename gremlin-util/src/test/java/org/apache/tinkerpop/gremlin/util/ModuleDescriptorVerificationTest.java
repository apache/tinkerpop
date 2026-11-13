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
package org.apache.tinkerpop.gremlin.util;

import org.junit.Test;

import java.lang.module.ModuleDescriptor;
import java.lang.module.ModuleFinder;
import java.nio.file.Paths;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ModuleDescriptorVerificationTest {

    @Test
    public void shouldHaveExpectedDescriptor() {
        final ModuleDescriptor descriptor = ModuleFinder.of(
                Paths.get(System.getProperty("gremlin.jpms.artifactJar"))).findAll().iterator().next().descriptor();
        assertFalse(descriptor.isAutomatic());
        assertEquals("org.apache.tinkerpop.gremlin.util", descriptor.name());
        assertTrue(descriptor.packages().contains("org.apache.tinkerpop.gremlin.util.ser.binary"));
        final Set<String> expectedExports = new TreeSet<>(descriptor.packages());
        expectedExports.remove("org.apache.tinkerpop.gremlin.util.ser.binary");
        assertEquals(expectedExports, descriptor.exports().stream().map(ModuleDescriptor.Exports::source).
                collect(Collectors.toCollection(TreeSet::new)));
        assertTrue(descriptor.exports().stream().noneMatch(ModuleDescriptor.Exports::isQualified));
        assertTrue(descriptor.opens().isEmpty());
        assertFalse(descriptor.isOpen());
        assertTrue(descriptor.uses().isEmpty());
        assertTrue(descriptor.provides().isEmpty());
    }
}
