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
package org.apache.tinkerpop.gremlin.structure.util;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * {@code gremlin-core} has no dependency on any concrete {@link Graph} implementation, so its own class loader never
 * sees a {@link DefaultGraphProvider} here. The tests vary the thread context class loader to check the fallback
 * lookup and that providers which can't be loaded are skipped rather than failing {@link DefaultGraphProvider#find()}.
 */
public class DefaultGraphProviderTest {

    private static final String SERVICES_FILE = "META-INF/services/" + DefaultGraphProvider.class.getName();

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void shouldFindNothingWhenNoProviderIsRegistered() {
        assertFalse(DefaultGraphProvider.find().isPresent());
    }

    @Test
    public void shouldSkipProviderThatIsNotASubtypeInContextClassLoader() throws Exception {
        final Optional<DefaultGraphProvider> provider = findWithContextServices(NotAProvider.class.getName());
        assertFalse(provider.isPresent());
    }

    @Test
    public void shouldFallBackToContextClassLoaderAndSkipBrokenProviders() throws Exception {
        final Optional<DefaultGraphProvider> provider = findWithContextServices(
                NotAProvider.class.getName(), "org.apache.tinkerpop.gremlin.structure.util.NoSuchProvider",
                TestProvider.class.getName());
        assertTrue(provider.isPresent());
        assertThat(provider.get(), instanceOf(TestProvider.class));
    }

    @Test
    public void shouldCountRepeatedRegistrationsOfTheSameProviderClassOnce() throws Exception {
        final Optional<DefaultGraphProvider> provider = findWithContextServices(
                TestProvider.class.getName(), TestProvider.class.getName());
        assertTrue(provider.isPresent());
        assertThat(provider.get(), instanceOf(TestProvider.class));
    }

    @Test
    public void shouldRejectMultipleDistinctProviderClasses() throws Exception {
        try {
            findWithContextServices(TestProvider.class.getName(), OtherTestProvider.class.getName());
            fail("Expected multiple distinct providers to fail");
        } catch (final IllegalStateException exception) {
            assertThat(exception.getMessage(), containsString(TestProvider.class.getName()));
            assertThat(exception.getMessage(), containsString(OtherTestProvider.class.getName()));
        }
    }

    /**
     * Calls {@link DefaultGraphProvider#find()} with a thread context class loader whose services file names the
     * given provider classes, restoring the original context class loader afterwards.
     */
    private Optional<DefaultGraphProvider> findWithContextServices(final String... providerClassNames) throws Exception {
        final File root = temp.newFolder();
        final File servicesFile = new File(root, SERVICES_FILE);
        assertTrue(servicesFile.getParentFile().mkdirs());
        Files.write(servicesFile.toPath(), List.of(providerClassNames), StandardCharsets.UTF_8);

        final Thread thread = Thread.currentThread();
        final ClassLoader original = thread.getContextClassLoader();
        try (URLClassLoader context = new URLClassLoader(new URL[]{root.toURI().toURL()},
                DefaultGraphProviderTest.class.getClassLoader())) {
            thread.setContextClassLoader(context);
            return DefaultGraphProvider.find();
        } finally {
            thread.setContextClassLoader(original);
        }
    }

    public static final class NotAProvider {
    }

    public static final class TestProvider implements DefaultGraphProvider {
        @Override
        public Graph open(final Configuration configuration) {
            throw new UnsupportedOperationException("not needed to test the lookup");
        }
    }

    public static final class OtherTestProvider implements DefaultGraphProvider {
        @Override
        public Graph open(final Configuration configuration) {
            throw new UnsupportedOperationException("not needed to test the lookup");
        }
    }
}
