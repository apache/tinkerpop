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
package org.apache.tinkerpop.gremlin.groovy.plugin;

import org.apache.tinkerpop.gremlin.groovy.loaders.SugarLoader;

/**
 * A plugin implementation which allows for the usage of Gremlin Groovy's syntactic sugar.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @deprecated As of release 3.2.4, replaced by {@link org.apache.tinkerpop.gremlin.groovy.jsr223.SugarGremlinPlugin}.
 */
@Deprecated
public class SugarGremlinPlugin extends AbstractGremlinPlugin {

    @Override
    public String getName() {
        return "tinkerpop.sugar";
    }

    /**
     * {@inheritDoc}
     * <p/>
     * Executes the {@link SugarLoader#load()} method in the {@link PluginAcceptor}.
     */
    @Override
    public void afterPluginTo(final PluginAcceptor pluginAcceptor) throws IllegalEnvironmentException, PluginInitializationException {
        try {
            pluginAcceptor.eval(SugarLoader.class.getPackage().getName() + "." + SugarLoader.class.getSimpleName() + ".load()");
        } catch (Exception ex) {
            if (io != null)
                io.out.println("Error loading the 'tinkerpop.sugar' plugin - " + ex.getMessage());
            else
                throw new PluginInitializationException("Error loading the 'tinkerpop.sugar' plugin - " + ex.getMessage(), ex);
        }
    }
}
