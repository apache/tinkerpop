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
package org.apache.tinkerpop.gremlin.structure.io.util;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * A registry name reaches this helper from the graph configuration, and on the OLAP path {@code io()} copies its
 * {@code with()} options into that configuration, so a name here is not necessarily operator-supplied. A name must
 * therefore be proven to be an {@link IoRegistry} before the class it names runs anything: this helper both invokes a
 * static factory method and, failing that, calls a no-arg constructor.
 */
public class IoRegistryHelperTest {

    @Test
    public void shouldNotInitializeANamedClassThatIsNotAnIoRegistry() {
        RegistryCanary.FIRED = false;

        try {
            // a class literal does not initialize the class it names, so naming the canary this way does not fire it
            IoRegistryHelper.createRegistries(Collections.singletonList(NotARegistry.class.getName()));
            fail("a class that is not an IoRegistry must not be accepted as one");
        } catch (IllegalStateException expected) {
            // the name is refused, and the assertion below is what makes the refusal meaningful
        }

        assertFalse("a class named as a registry must not be initialized, constructed, or have a method invoked on " +
                "it before it is known to be an IoRegistry", RegistryCanary.FIRED);
    }

    @Test
    public void shouldNotInitializeANamedClassThatIsNotAnIoRegistryFromConfiguration() {
        RegistryCanary.FIRED = false;

        final Configuration conf = new BaseConfiguration();
        conf.setProperty(IoRegistry.IO_REGISTRY, NotARegistry.class.getName());

        try {
            IoRegistryHelper.createRegistries(conf);
            fail("a class that is not an IoRegistry must not be accepted as one");
        } catch (IllegalStateException expected) {
            // as above
        }

        assertFalse("the configuration form must refuse the name on the same terms", RegistryCanary.FIRED);
    }

    /**
     * The forms the helper is documented to accept have to keep working: an instance, a {@link Class}, a class name
     * with a static {@code instance()}, and a class name with only a no-arg constructor.
     */
    @Test
    public void shouldCreateRegistriesFromTheAcceptedForms() {
        final List<IoRegistry> registries = IoRegistryHelper.createRegistries(Arrays.asList(
                new ConstructorOnlyRegistry(),
                ConstructorOnlyRegistry.class,
                StaticInstanceRegistry.class.getName(),
                ConstructorOnlyRegistry.class.getName()));

        assertEquals(4, registries.size());
        assertThat(registries.get(0), instanceOf(ConstructorOnlyRegistry.class));
        assertThat(registries.get(1), instanceOf(ConstructorOnlyRegistry.class));
        assertThat(registries.get(2), instanceOf(StaticInstanceRegistry.class));
        assertThat(registries.get(3), instanceOf(ConstructorOnlyRegistry.class));
    }

    @Test
    public void shouldReturnEmptyForAConfigurationWithoutARegistry() {
        assertEquals(Collections.emptyList(), IoRegistryHelper.createRegistries(new BaseConfiguration()));
    }

    /**
     * Holds the flag {@link NotARegistry} sets. Kept out of that class so that reading the flag does not initialize
     * the class the test asserts was never initialized.
     */
    public static class RegistryCanary {
        static volatile boolean FIRED = false;
    }

    /**
     * Not an {@link IoRegistry}. Its static initializer, its constructor and both factory method names the helper
     * looks for all report, since the helper can reach a named class through any of them.
     */
    public static class NotARegistry {
        static {
            RegistryCanary.FIRED = true;
        }

        public NotARegistry() {
            RegistryCanary.FIRED = true;
        }

        public static Object instance() {
            RegistryCanary.FIRED = true;
            return null;
        }

        public static Object getInstance() {
            RegistryCanary.FIRED = true;
            return null;
        }
    }

    public static class StaticInstanceRegistry extends AbstractIoRegistry {
        private static final StaticInstanceRegistry INSTANCE = new StaticInstanceRegistry();

        public static StaticInstanceRegistry instance() {
            return INSTANCE;
        }
    }

    public static class ConstructorOnlyRegistry extends AbstractIoRegistry {
        public ConstructorOnlyRegistry() {
        }
    }
}
