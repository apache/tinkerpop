/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.tinkeradoc;

import org.asciidoctor.Asciidoctor;
import org.asciidoctor.Options;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Verifies that the AsciidoctorJ extensions register via SPI and process documents correctly.
 */
public class GremlinDocsTest {

    @Test
    public void shouldRegisterExtensionsViaSpi() {
        // SPI auto-registration happens when Asciidoctor is created
        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            assertThat(asciidoctor, is(notNullValue()));

            // Convert a simple doc to verify no errors from extension registration
            final String result = asciidoctor.convert("= Test\n\nHello", Options.builder().build());
            assertThat(result, is(notNullValue()));
        }
    }

    @Test
    public void shouldPassthroughPostprocessor() {
        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            final String input = "= Test\n\nSome content here.";
            final String result = asciidoctor.convert(input, Options.builder().build());
            // Postprocessor is a no-op, so output should contain the content
            assertThat(result.contains("Some content here"), is(true));
        }
    }

    @Test
    public void shouldFindGremlinGroovyListingBlocks() {
        final GremlinTreeprocessor treeprocessor = new GremlinTreeprocessor();
        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.unregisterAllExtensions();
            asciidoctor.javaExtensionRegistry().treeprocessor(treeprocessor);
            final String input = "= Test\n\n[gremlin-groovy]\n----\ng.V()\n----\n";
            final String result = asciidoctor.convert(input, Options.builder().build());
            assertThat(result, is(notNullValue()));
            assertThat(treeprocessor.getGremlinBlockCount(), is(1));
        }
    }

    @Test
    public void shouldIgnoreNonGremlinListingBlocks() {
        final GremlinTreeprocessor treeprocessor = new GremlinTreeprocessor();
        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.unregisterAllExtensions();
            asciidoctor.javaExtensionRegistry().treeprocessor(treeprocessor);
            final String input = "= Test\n\n[source,java]\n----\nSystem.out.println();\n----\n";
            final String result = asciidoctor.convert(input, Options.builder().build());
            assertThat(result, is(notNullValue()));
            assertThat(treeprocessor.getGremlinBlockCount(), is(0));
        }
    }
}
