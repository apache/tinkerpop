package org.apache.tinkerpop.gremlin.groovy.jsr223.ast

import groovy.transform.CompilationUnitAware
import org.codehaus.groovy.ast.ASTNode
import org.codehaus.groovy.ast.ClassNode
import org.codehaus.groovy.classgen.GeneratorContext
import org.codehaus.groovy.control.SourceUnit
import org.codehaus.groovy.control.customizers.ASTTransformationCustomizer
import org.codehaus.groovy.transform.ASTTransformation

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

/**
 * Overrides the single application of the specified global AST Transformation (which is how the base
 * {@code ASTTransformationCustomizer} works. That approach is not so helpful in the ScriptEngine where we want
 * each script to be treated as an independent source unit. This is a bit of a hack but the use case here is fairly
 * narrow
 */
class RepeatASTTransformationCustomizer extends ASTTransformationCustomizer {
    RepeatASTTransformationCustomizer(final ASTTransformation transformation) {
        super(transformation)
    }

    @Override
    @SuppressWarnings('Instanceof')
    void call(SourceUnit source, GeneratorContext context, ClassNode classNode) {
        if (transformation instanceof CompilationUnitAware) {
            transformation.compilationUnit = compilationUnit
        }

        // this is a global AST transformation
        transformation.visit(null, source)
    }
}
