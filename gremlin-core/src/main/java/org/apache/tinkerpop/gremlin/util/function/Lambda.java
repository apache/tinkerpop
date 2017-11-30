/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.util.function;

import java.io.Serializable;
import java.util.Comparator;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Lambda extends Serializable {

    public String getLambdaScript();

    public String getLambdaLanguage();

    public int getLambdaArguments();

    public abstract static class AbstractLambda implements Lambda {
        private final String lambdaSource;
        private final String lambdaLanguage;
        private final int lambdaArguments;

        private AbstractLambda(final String lambdaSource, final String lambdaLanguage, final int lambdaArguments) {
            this.lambdaSource = lambdaSource;
            this.lambdaLanguage = lambdaLanguage;
            this.lambdaArguments = lambdaArguments;
        }

        @Override
        public String getLambdaScript() {
            return this.lambdaSource;
        }

        @Override
        public String getLambdaLanguage() {
            return this.lambdaLanguage;
        }

        @Override
        public int getLambdaArguments() {
            return this.lambdaArguments;
        }

        @Override
        public String toString() {
            return "lambda[" + this.lambdaSource + "]";
        }

        @Override
        public int hashCode() {
            return this.lambdaSource.hashCode() + this.lambdaLanguage.hashCode() + this.lambdaArguments;
        }

        @Override
        public boolean equals(final Object object) {
            return object instanceof Lambda &&
                    ((Lambda) object).getLambdaArguments() == this.getLambdaArguments() &&
                    ((Lambda) object).getLambdaScript().equals(this.lambdaSource) &&
                    ((Lambda) object).getLambdaLanguage().equals(this.lambdaLanguage);
        }
    }

    public static class UnknownArgLambda extends AbstractLambda {

        public UnknownArgLambda(final String lambdaSource, final String lambdaLanguage, final int lambdaArguments) {
            super(lambdaSource, lambdaLanguage, lambdaArguments);
        }
    }

    public static class ZeroArgLambda<A> extends AbstractLambda implements Supplier<A> {

        public ZeroArgLambda(final String lambdaSource, final String lambdaLanguage) {
            super(lambdaSource, lambdaLanguage, 0);
        }

        @Override
        public A get() {
            return null;
        }

    }

    public static class OneArgLambda<A, B> extends AbstractLambda implements Function<A, B>, Predicate<A>, Consumer<A> {

        public OneArgLambda(final String lambdaSource, final String lambdaLanguage) {
            super(lambdaSource, lambdaLanguage, 1);
        }

        @Override
        public B apply(final A a) {
            return null;
        }

        @Override
        public boolean test(final A a) {
            return false;
        }

        @Override
        public void accept(final A a) {

        }
    }

    public static class TwoArgLambda<A, B, C> extends AbstractLambda implements BiFunction<A, B, C>, Comparator<A> {

        public TwoArgLambda(final String lambdaSource, final String lambdaLanguage) {
            super(lambdaSource, lambdaLanguage, 2);
        }

        @Override
        public C apply(final A a, final B b) {
            return null;
        }


        @Override
        public int compare(final A first, final A second) {
            return 0;
        }
    }

    ///

    public static String DEFAULT_LAMBDA_LANGUAGE = "gremlin-groovy";

    public static <A, B> Function<A, B> function(final String lambdaSource, final String lambdaLanguage) {
        return new OneArgLambda<>(lambdaSource, lambdaLanguage);
    }

    public static <A> Predicate<A> predicate(final String lambdaSource, final String lambdaLanguage) {
        return new OneArgLambda<>(lambdaSource, lambdaLanguage);
    }

    public static <A> Consumer<A> consumer(final String lambdaSource, final String lambdaLanguage) {
        return new OneArgLambda<>(lambdaSource, lambdaLanguage);
    }

    public static <A> Supplier<A> supplier(final String lambdaSource, final String lambdaLanguage) {
        return new ZeroArgLambda<>(lambdaSource, lambdaLanguage);
    }

    public static <A> Comparator<A> comparator(final String lambdaSource, final String lambdaLanguage) {
        return new TwoArgLambda<>(lambdaSource, lambdaLanguage);
    }

    public static <A, B, C> BiFunction<A, B, C> biFunction(final String lambdaSource, final String lambdaLanguage) {
        return new TwoArgLambda<>(lambdaSource, lambdaLanguage);
    }

    //

    public static <A, B> Function<A, B> function(final String lambdaSource) {
        return new OneArgLambda<>(lambdaSource, DEFAULT_LAMBDA_LANGUAGE);
    }

    public static <A> Predicate<A> predicate(final String lambdaSource) {
        return new OneArgLambda<>(lambdaSource, DEFAULT_LAMBDA_LANGUAGE);
    }

    public static <A> Consumer<A> consumer(final String lambdaSource) {
        return new OneArgLambda<>(lambdaSource, DEFAULT_LAMBDA_LANGUAGE);
    }

    public static <A> Supplier<A> supplier(final String lambdaSource) {
        return new ZeroArgLambda<>(lambdaSource, DEFAULT_LAMBDA_LANGUAGE);
    }

    public static <A> Comparator<A> comparator(final String lambdaSource) {
        return new TwoArgLambda<>(lambdaSource, DEFAULT_LAMBDA_LANGUAGE);
    }

    public static <A, B, C> BiFunction<A, B, C> biFunction(final String lambdaSource) {
        return new TwoArgLambda<>(lambdaSource, DEFAULT_LAMBDA_LANGUAGE);
    }
}
