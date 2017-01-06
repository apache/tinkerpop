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

import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Test;

import java.lang.reflect.Method;
import java.time.DayOfWeek;
import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DefaultImportCustomizerTest {
    @Test
    public void shouldReturnAssignedImports() throws Exception {
        final Method abs = Math.class.getMethod("abs", double.class);
        final Enum dayOfWeekEnum = DayOfWeek.SATURDAY;
        final Enum tEnum = T.id;
        final ImportCustomizer imports = DefaultImportCustomizer.build()
                .addClassImports(java.awt.Color.class, java.awt.AlphaComposite.class)
                .addMethodImports(abs)
                .addEnumImports(dayOfWeekEnum, tEnum).create();

        assertEquals(2, imports.getClassImports().size());
        assertThat(imports.getClassImports(), hasItems(java.awt.Color.class, java.awt.AlphaComposite.class));

        assertEquals(1, imports.getMethodImports().size());
        assertThat(imports.getMethodImports(), hasItems(abs));

        assertEquals(2, imports.getEnumImports().size());
        assertThat(imports.getEnumImports(), hasItems(dayOfWeekEnum, tEnum));
    }

    @Test
    public void shouldReturnAssignedImportsWhenBuiltViaCollections() throws Exception {
        final Method abs = Math.class.getMethod("abs", double.class);
        final Enum dayOfWeekEnum = DayOfWeek.SATURDAY;
        final Enum tEnum = T.id;
        final ImportCustomizer imports = DefaultImportCustomizer.build()
                .addClassImports(Arrays.asList(java.awt.Color.class, java.awt.AlphaComposite.class))
                .addMethodImports(Collections.singletonList(abs))
                .addEnumImports(Arrays.asList(dayOfWeekEnum, tEnum)).create();

        assertEquals(2, imports.getClassImports().size());
        assertThat(imports.getClassImports(), hasItems(java.awt.Color.class, java.awt.AlphaComposite.class));

        assertEquals(1, imports.getMethodImports().size());
        assertThat(imports.getMethodImports(), hasItems(abs));

        assertEquals(2, imports.getEnumImports().size());
        assertThat(imports.getEnumImports(), hasItems(dayOfWeekEnum, tEnum));
    }
}
