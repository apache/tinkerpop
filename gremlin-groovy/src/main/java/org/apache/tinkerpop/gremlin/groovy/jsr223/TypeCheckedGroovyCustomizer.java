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

import groovy.transform.TypeChecked;
import org.apache.tinkerpop.gremlin.groovy.CompilerCustomizerProvider;
import org.codehaus.groovy.control.customizers.ASTTransformationCustomizer;
import org.codehaus.groovy.control.customizers.CompilationCustomizer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Injects the {@code TypeChecked} transformer to enable type validation on script execution.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class TypeCheckedGroovyCustomizer implements GroovyCustomizer {

    private final String extensions;

    TypeCheckedGroovyCustomizer() {
        this(null);
    }

    /**
     * Configures the {@code TypeChecked} annotation to use optional extensions.  The argument should be one or more
     * groovy scripts on the classpath or the fully qualified classname of a precompiled extension.  If there are
     * multiple extensions then extensions should be comma separated.
     */
    TypeCheckedGroovyCustomizer(final String extensions) {
        this.extensions = extensions;
    }

    @Override
    public CompilationCustomizer create() {
        final Map<String, Object> annotationParams = new HashMap<>();
        if (extensions != null && !extensions.isEmpty()) {
            if (extensions.contains(","))
                annotationParams.put("extensions", Stream.of(extensions.split(",")).collect(Collectors.toList()));
            else
                annotationParams.put("extensions", Collections.singletonList(extensions));
        }
        return new ASTTransformationCustomizer(annotationParams, TypeChecked.class);
    }
}
