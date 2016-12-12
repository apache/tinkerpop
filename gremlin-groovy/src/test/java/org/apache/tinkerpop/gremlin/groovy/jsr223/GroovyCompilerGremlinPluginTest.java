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

import org.apache.tinkerpop.gremlin.groovy.jsr223.customizer.CompileStaticCustomizerProvider;
import org.apache.tinkerpop.gremlin.groovy.jsr223.customizer.ConfigurationCustomizerProvider;
import org.apache.tinkerpop.gremlin.groovy.jsr223.customizer.InterpreterModeCustomizerProvider;
import org.apache.tinkerpop.gremlin.groovy.jsr223.customizer.ThreadInterruptCustomizerProvider;
import org.apache.tinkerpop.gremlin.groovy.jsr223.customizer.TimedInterruptCustomizerProvider;
import org.apache.tinkerpop.gremlin.groovy.jsr223.customizer.TypeCheckedCustomizerProvider;
import org.apache.tinkerpop.gremlin.jsr223.Customizer;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.tinkerpop.gremlin.groovy.jsr223.GroovyCompilerGremlinPlugin.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GroovyCompilerGremlinPluginTest {

    @Test
    public void shouldConfigureForGroovyOnly() {
        final GroovyCompilerGremlinPlugin plugin = GroovyCompilerGremlinPlugin.build().
                compilation(Compilation.COMPILE_STATIC).create();
        final Optional<Customizer[]> customizers = plugin.getCustomizers("gremlin-not-real");
        assertThat(customizers.isPresent(), is(false));
    }

    @Test
    public void shouldConfigureWithCompileStatic() {
        final GroovyCompilerGremlinPlugin plugin = GroovyCompilerGremlinPlugin.build().
                compilation(Compilation.COMPILE_STATIC).create();
        final Optional<Customizer[]> customizers = plugin.getCustomizers("gremlin-groovy");
        assertThat(customizers.isPresent(), is(true));
        assertEquals(1, customizers.get().length);
        assertThat(customizers.get()[0], instanceOf(CompileStaticGroovyCustomizer.class));
    }

    @Test
    public void shouldConfigureWithTypeChecked() {
        final GroovyCompilerGremlinPlugin plugin = GroovyCompilerGremlinPlugin.build().
                compilation(Compilation.TYPE_CHECKED).create();
        final Optional<Customizer[]> customizers = plugin.getCustomizers("gremlin-groovy");
        assertThat(customizers.isPresent(), is(true));
        assertEquals(1, customizers.get().length);
        assertThat(customizers.get()[0], instanceOf(TypeCheckedGroovyCustomizer.class));
    }

    @Test
    public void shouldConfigureWithCustomCompilerConfigurations() {
        final Map<String,Object> conf = new HashMap<>();
        conf.put("Debug", true);
        final GroovyCompilerGremlinPlugin plugin = GroovyCompilerGremlinPlugin.build().
                compilerConfigurationOptions(conf).create();
        final Optional<Customizer[]> customizers = plugin.getCustomizers("gremlin-groovy");
        assertThat(customizers.isPresent(), is(true));
        assertEquals(1, customizers.get().length);
        assertThat(customizers.get()[0], instanceOf(ConfigurationGroovyCustomizer.class));

        final CompilerConfiguration compilerConfiguration = new CompilerConfiguration();
        assertThat(compilerConfiguration.getDebug(), is(false));

        final ConfigurationGroovyCustomizer provider = (ConfigurationGroovyCustomizer) customizers.get()[0];
        provider.applyCustomization(compilerConfiguration);

        assertThat(compilerConfiguration.getDebug(), is(true));
    }

    @Test
    public void shouldConfigureWithInterpreterMode() {
        final GroovyCompilerGremlinPlugin plugin = GroovyCompilerGremlinPlugin.build().
                enableInterpreterMode(true).create();
        final Optional<Customizer[]> customizers = plugin.getCustomizers("gremlin-groovy");
        assertThat(customizers.isPresent(), is(true));
        assertEquals(1, customizers.get().length);
        assertThat(customizers.get()[0], instanceOf(InterpreterModeGroovyCustomizer.class));
    }

    @Test
    public void shouldConfigureWithThreadInterrupt() {
        final GroovyCompilerGremlinPlugin plugin = GroovyCompilerGremlinPlugin.build().
                enableThreadInterrupt(true).create();
        final Optional<Customizer[]> customizers = plugin.getCustomizers("gremlin-groovy");
        assertThat(customizers.isPresent(), is(true));
        assertEquals(1, customizers.get().length);
        assertThat(customizers.get()[0], instanceOf(ThreadInterruptGroovyCustomizer.class));
    }

    @Test
    public void shouldConfigureWithTimedInterrupt() {
        final GroovyCompilerGremlinPlugin plugin = GroovyCompilerGremlinPlugin.build().
                timedInterrupt(60000).create();
        final Optional<Customizer[]> customizers = plugin.getCustomizers("gremlin-groovy");
        assertThat(customizers.isPresent(), is(true));
        assertEquals(1, customizers.get().length);
        assertThat(customizers.get()[0], instanceOf(TimedInterruptGroovyCustomizer.class));
    }

    @Test(expected = IllegalStateException.class)
    public void shouldNotConfigureIfNoSettingsAreSupplied() {
        GroovyCompilerGremlinPlugin.build().create();
    }
}
