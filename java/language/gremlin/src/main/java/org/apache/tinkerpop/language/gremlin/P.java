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
package org.apache.tinkerpop.language.gremlin;

import org.apache.tinkerpop.machine.bytecode.compiler.Pred;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class P<S> {

    private final S object;
    private final Pred pred;

    private P(final Pred pred, final S object) {
        this.pred = pred;
        this.object = object;
    }

    public S object() {
        return this.object;
    }

    public Pred type() {
        return this.pred;
    }

    public static <S> P<S> eq(final Object object) {
        return new P<>(Pred.eq, (S) object);
    }

    public static <S> P<S> neq(final Object object) {
        return new P<>(Pred.neq, (S) object);
    }

    public static <S> P<S> lt(final Object object) {
        return new P<>(Pred.lt, (S) object);
    }

    public static <S> P<S> gt(final Object object) {
        return new P<>(Pred.gt, (S) object);
    }

    public static <S> P<S> lte(final Object object) {
        return new P<>(Pred.lte, (S) object);
    }

    public static <S> P<S> gte(final Object object) {
        return new P<>(Pred.gte, (S) object);
    }

    public static <S> P<S> regex(final Object object) {
        return new P<>(Pred.regex, (S) object);
    }

    @Override
    public String toString() {
        return this.pred.toString();
    }


}
