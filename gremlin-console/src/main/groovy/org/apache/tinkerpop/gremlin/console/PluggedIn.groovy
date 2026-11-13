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
package org.apache.tinkerpop.gremlin.console

import org.apache.groovy.groovysh.jline.GroovyEngine
import org.apache.tinkerpop.gremlin.jsr223.BindingsCustomizer
import org.apache.tinkerpop.gremlin.jsr223.GremlinPlugin
import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer
import org.apache.tinkerpop.gremlin.jsr223.ScriptCustomizer

/**
 * A plugin available to the console, applied to its Groovy engine when activated. Being active now and after a restart
 * can differ, as a running engine cannot drop a plugin and some installed plugins need a restart.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class PluggedIn {
    private static final String LINE_SEPARATOR = System.getProperty("line.separator")
    private final GremlinPlugin plugin
    private final GroovyEngine engine

    // applied to the engine of this session
    private boolean loaded
    // listed in ext/plugins.txt, so active after a restart
    private boolean activated
    // installed in this session and only usable after a restart
    private boolean restartRequired = false

    public PluggedIn(final GremlinPlugin plugin, final GroovyEngine engine, final boolean activated) {
        this.plugin = plugin
        this.loaded = activated
        this.activated = activated
        this.engine = engine
    }

    GremlinPlugin getPlugin() {
        return plugin
    }

    boolean getActivated() {
        return activated
    }

    boolean getLoaded() {
        return loaded
    }

    boolean getRestartRequired() {
        return restartRequired
    }

    void setRestartRequired(final boolean restartRequired) {
        this.restartRequired = restartRequired
    }

    /**
     * Applies the plugin to the engine unless it is loaded already or needs a restart, and keeps it for the next start.
     */
    void activate() {
        if (!loaded && !restartRequired) {
            plugin.getCustomizers("gremlin-groovy").get().each {
                if (it instanceof ImportCustomizer) {
                    org.codehaus.groovy.control.customizers.ImportCustomizer ic = new org.codehaus.groovy.control.customizers.ImportCustomizer()
                    ic.addStarImports(it.getClassPackages().collect() { it.getName() }.toArray(new String[0]))
                    ic.addStaticStars(it.getMethodClasses().collect() { it.getCanonicalName() }.toArray(new String[0]))
                    ic.addStaticStars(it.getEnumClasses().collect() { it.getCanonicalName() }.toArray(new String[0]))
                    ic.addStaticStars(it.getFieldClasses().collect() { it.getCanonicalName() }.toArray(new String[0]))
                    engine.getCompilerConfiguration().addCompilationCustomizers(ic)
                } else if (it instanceof ScriptCustomizer) {
                    it.getScripts().collect { it.join(LINE_SEPARATOR) }.each { engine.execute(it) }
                } else if (it instanceof BindingsCustomizer) {
                    it.bindings.entrySet().each { kv -> engine.put(kv.key, kv.value) }
                }
            }
            this.loaded = true
        }
        this.activated = true
    }

    /**
     * Leaves the plugin out of the next start. It stays loaded in this session.
     */
    void deactivate() {
        this.activated = false
    }
}
