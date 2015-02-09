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
package com.tinkerpop.gremlin.groovy;

import org.codehaus.groovy.control.customizers.CompilationCustomizer;
import org.codehaus.groovy.control.customizers.ImportCustomizer;

import java.util.Collections;
import java.util.Set;

/**
 * Provides no imports.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class NoImportCustomizerProvider implements ImportCustomizerProvider {
    @Override
    public CompilationCustomizer getCompilationCustomizer() {
        return new ImportCustomizer();
    }

    @Override
    public Set<String> getExtraImports() {
        return Collections.emptySet();
    }

    @Override
    public Set<String> getExtraStaticImports() {
        return Collections.emptySet();
    }

    @Override
    public Set<String> getImports() {
        return Collections.emptySet();
    }

    @Override
    public Set<String> getStaticImports() {
        return Collections.emptySet();
    }
}
