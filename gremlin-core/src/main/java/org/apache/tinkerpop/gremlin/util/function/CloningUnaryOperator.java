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
package org.apache.tinkerpop.gremlin.util.function;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.function.UnaryOperator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class CloningUnaryOperator<A> implements UnaryOperator<A>, Serializable {

    private static final CloningUnaryOperator INSTANCE = new CloningUnaryOperator();

    private CloningUnaryOperator() {
    }

    @Override
    public A apply(final A a) {
        if (a instanceof HashMap)
            return (A) ((HashMap) a).clone();
        else if (a instanceof HashSet)
            return (A) ((HashSet) a).clone();
        else
            throw new IllegalArgumentException("The provided class is not registered for cloning: " + a.getClass());
    }

    public static <A> CloningUnaryOperator<A> instance() {
        return INSTANCE;
    }
}
