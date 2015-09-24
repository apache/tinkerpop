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

import org.codehaus.groovy.ast.MethodNode;
import org.codehaus.groovy.ast.expr.Expression;
import org.codehaus.groovy.transform.stc.AbstractTypeCheckingExtension;
import org.codehaus.groovy.transform.stc.StaticTypeCheckingVisitor;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class PrecompiledExtensions  {
    public static class PreventColorUsageExtension extends AbstractTypeCheckingExtension {

        public PreventColorUsageExtension(final StaticTypeCheckingVisitor typeCheckingVisitor) {
            super(typeCheckingVisitor);
        }

        @Override
        public void onMethodSelection(final Expression expression, final MethodNode target) {
            if (target.getDeclaringClass().getName().equals("java.awt.Color")) {
                addStaticTypeError("Method call is not allowed!", expression);
            }
        }
    }

    public static class PreventCountDownLatchUsageExtension extends AbstractTypeCheckingExtension {

        public PreventCountDownLatchUsageExtension(final StaticTypeCheckingVisitor typeCheckingVisitor) {
            super(typeCheckingVisitor);
        }

        @Override
        public void onMethodSelection(final Expression expression, final MethodNode target) {
            if (target.getDeclaringClass().getName().equals("java.util.concurrent.CountDownLatch")) {
                addStaticTypeError("Method call is not allowed!", expression);
            }
        }

    }
}
