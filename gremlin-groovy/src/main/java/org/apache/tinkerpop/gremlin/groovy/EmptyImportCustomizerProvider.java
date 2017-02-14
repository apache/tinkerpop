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

import org.codehaus.groovy.control.customizers.CompilationCustomizer;
import org.codehaus.groovy.control.customizers.ImportCustomizer;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * This {@link ImportCustomizerProvider} is empty and comes with no pre-defined imports at all.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @deprecated As of release 3.2.5, replaced by {@link ImportCustomizer}
 */
@Deprecated
public class EmptyImportCustomizerProvider extends DefaultImportCustomizerProvider {

    /**
     * Utilizes imports defined by the supplied arguments.
     */
    public EmptyImportCustomizerProvider(final Set<String> extraImports, final Set<String> extraStaticImports) {
        this.extraStaticImports.addAll(extraStaticImports);
        this.extraImports.addAll(extraImports);
    }

    /**
     * Utilizes imports defined by the supplied arguments.
     *
     * @param baseCustomizer Imports from this customizer get added to the new one.
     */
    public EmptyImportCustomizerProvider(final ImportCustomizerProvider baseCustomizer,
                                         final Set<String> extraImports, final Set<String> extraStaticImports) {
        this(extraImports, extraStaticImports);
        this.extraImports.addAll(baseCustomizer.getImports());
        this.extraImports.addAll(baseCustomizer.getExtraImports());
        this.extraStaticImports.addAll(baseCustomizer.getStaticImports());
        this.extraStaticImports.addAll(baseCustomizer.getExtraStaticImports());
    }

    @Override
    public CompilationCustomizer create() {
        final ImportCustomizer ic = new ImportCustomizer();

        processImports(ic, extraImports);
        processStaticImports(ic, extraStaticImports);

        return ic;
    }

    @Override
    public Set<String> getImports() {
        return Collections.emptySet();
    }

    @Override
    public Set<String> getStaticImports() {
        return Collections.emptySet();
    }

    @Override
    public Set<String> getAllImports() {
        final Set<String> allImports = new HashSet<>();
        allImports.addAll(extraImports);
        allImports.addAll(extraStaticImports);

        return allImports;
    }
}
