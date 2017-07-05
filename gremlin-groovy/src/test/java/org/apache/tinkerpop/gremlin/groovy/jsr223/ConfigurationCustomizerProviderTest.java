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

import org.codehaus.groovy.control.CompilerConfiguration;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ConfigurationCustomizerProviderTest {
    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionForNoSettings() {
        new ConfigurationGroovyCustomizer();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionForInvalidSettings() {
        new ConfigurationGroovyCustomizer("only-one-arg");
    }

    @Test
    public void shouldThrowExceptionForNotFoundSetting() {
        final CompilerConfiguration configuration = new CompilerConfiguration();
        try {
            final ConfigurationGroovyCustomizer provider = new ConfigurationGroovyCustomizer(
                    "Tolerance", 3,
                    "NotRealSettingThatWouldEverOccur2", new java.util.Date());

            provider.applyCustomization(configuration);
        } catch (Exception ex) {
            assertThat(ex, instanceOf(IllegalStateException.class));
            assertEquals("Invalid setting [NotRealSettingThatWouldEverOccur2] for CompilerConfiguration", ex.getMessage());
        }
    }

    @Test
    public void shouldApplyConfigurationChanges() {
        final CompilerConfiguration configuration = new CompilerConfiguration();

        assertEquals(10, configuration.getTolerance());
        assertNull(configuration.getScriptBaseClass());
        assertEquals(false, configuration.getDebug());

        final ConfigurationGroovyCustomizer provider = new ConfigurationGroovyCustomizer(
                "Tolerance", 3,
                "ScriptBaseClass", "Something",
                "Debug", true);

        provider.applyCustomization(configuration);

        assertEquals(3, configuration.getTolerance());
        assertEquals("Something", configuration.getScriptBaseClass());
        assertEquals(true, configuration.getDebug());
    }
}
