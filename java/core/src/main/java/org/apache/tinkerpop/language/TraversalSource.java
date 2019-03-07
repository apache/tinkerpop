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
package org.apache.tinkerpop.language;

import org.apache.tinkerpop.machine.coefficients.Coefficients;
import org.apache.tinkerpop.machine.coefficients.LongCoefficients;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalSource<C> {

    private Coefficients<C> coefficients = (Coefficients) new LongCoefficients();

    protected TraversalSource() {
    }

    public TraversalSource<C> coefficients(final Coefficients coefficients) {
        this.coefficients = coefficients;
        return this;
    }

    public <A> Traversal<C, A, A> inject(final A... objects) {
        final Traversal<C, A, A> traversal = new Traversal<>(this.coefficients.unity());
        return traversal.inject(objects);
    }
}
