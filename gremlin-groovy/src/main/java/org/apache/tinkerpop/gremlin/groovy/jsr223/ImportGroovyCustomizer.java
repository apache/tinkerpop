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

import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.codehaus.groovy.control.customizers.CompilationCustomizer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class ImportGroovyCustomizer implements GroovyCustomizer {

    private final List<ImportCustomizer> customizers;

    ImportGroovyCustomizer(final ImportCustomizer... customizers) {
        this.customizers = Arrays.asList(customizers);
    }

    ImportGroovyCustomizer(final ImportGroovyCustomizer ic, final ImportCustomizer... customizers) {
        this.customizers = new ArrayList<>(Arrays.asList(customizers));
        this.customizers.addAll(ic.customizers);
    }

    @Override
    public CompilationCustomizer create() {
        final org.codehaus.groovy.control.customizers.ImportCustomizer ic = new org.codehaus.groovy.control.customizers.ImportCustomizer();

        // there's something weird in groovy about doing specific imports instead of wildcard imports. with wildcard
        // imports groovy seems to allow methods to be overloaded with enums such that things like Column.values and
        // __.values() can be resolved by the compiler. if they are both directly in the imports then one or the other
        // can't be found. the temporary fix is to hardcode a wildcard import __ and then filter out the core imports
        // from the incoming customizer. ultimately, the fix should be to resolve the naming conflicts to ensure a
        // unique space somehow.
        ic.addStaticStars(__.class.getCanonicalName());
        for (ImportCustomizer customizer : customizers) {
            customizer.getClassImports().forEach(i -> ic.addImports(i.getCanonicalName()));
            customizer.getMethodImports().stream()
                    .filter(m -> !m.getDeclaringClass().equals(__.class))
                    .forEach(m -> ic.addStaticImport(m.getDeclaringClass().getCanonicalName(), m.getName()));
            customizer.getEnumImports().forEach(m -> ic.addStaticImport(m.getDeclaringClass().getCanonicalName(), m.name()));
        }

        return ic;
    }
}
