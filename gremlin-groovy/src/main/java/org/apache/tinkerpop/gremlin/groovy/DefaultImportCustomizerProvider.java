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
package org.apache.tinkerpop.gremlin.groovy;

import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer;

import java.util.HashSet;
import java.util.Set;

/**
 * Grabs the standard Gremlin core classes and allows additional imports to be added.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @deprecated As of release 3.2.5, replaced by {@link DefaultImportCustomizer}.
 */
@Deprecated
public class DefaultImportCustomizerProvider extends AbstractImportCustomizerProvider {
    private static Set<String> staticExtraImports = new HashSet<>();
    private static Set<String> staticExtraStaticImports = new HashSet<>();

    /**
     * Utilizes imports defined statically by initializeStatically().
     */
    public DefaultImportCustomizerProvider() {
        this.extraImports.addAll(staticExtraImports);
        this.extraStaticImports.addAll(staticExtraStaticImports);
    }

    /**
     * Utilizes imports defined by the supplied arguments.  Those imports defined statically through
     * initializeStatically() are ignored.
     */
    public DefaultImportCustomizerProvider(final Set<String> extraImports, final Set<String> extraStaticImports) {
        this.extraStaticImports.addAll(extraStaticImports);
        this.extraImports.addAll(extraImports);
    }

    /**
     * Utilizes imports defined by the supplied arguments.  Those imports defined statically through
     * initializeStatically() are ignored.
     *
     * @param baseCustomizer Imports from this customizer get added to the new one
     */
    public DefaultImportCustomizerProvider(final ImportCustomizerProvider baseCustomizer,
                                           final Set<String> extraImports, final Set<String> extraStaticImports) {
        this(extraImports, extraStaticImports);
        this.extraImports.addAll(baseCustomizer.getExtraImports());
        this.extraStaticImports.addAll(baseCustomizer.getExtraStaticImports());
    }

    /**
     * Allows imports to defined globally and statically. This method must be called prior to initialization of
     * a ScriptEngine instance through the ScriptEngineFactory.
     */
    public static void initializeStatically(final Set<String> extraImports, final Set<String> extraStaticImports) {
        if (extraImports != null) {
            staticExtraImports = extraImports;
        }

        if (extraStaticImports != null) {
            staticExtraStaticImports = extraStaticImports;
        }
    }
}