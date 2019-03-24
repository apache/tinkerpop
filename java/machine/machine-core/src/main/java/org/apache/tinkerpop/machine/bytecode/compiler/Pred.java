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
package org.apache.tinkerpop.machine.bytecode.compiler;

import org.apache.tinkerpop.machine.util.NumberHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public enum Pred {

    eq {
        public boolean test(final Object first, final Object second) {
            return null == first ? null == second : (first instanceof Number && second instanceof Number
                    ? NumberHelper.compare((Number) first, (Number) second) == 0
                    : first.equals(second));
        }
    }, neq {
        public boolean test(final Object first, final Object second) {
            return !eq.test(first, second);
        }
    }, lt {
        public boolean test(final Object first, final Object second) {
            return null != first && null != second && (
                    first instanceof Number && second instanceof Number
                            ? NumberHelper.compare((Number) first, (Number) second) < 0
                            : ((Comparable) first).compareTo(second) < 0);
        }
    }, gt {
        public boolean test(final Object first, final Object second) {
            return null != first && null != second && (
                    first instanceof Number && second instanceof Number
                            ? NumberHelper.compare((Number) first, (Number) second) > 0
                            : ((Comparable) first).compareTo(second) > 0);
        }
    }, lte {
        public boolean test(final Object first, final Object second) {
            return null == first ? null == second : (null != second && !gt.test(first, second));
        }
    }, gte {
        public boolean test(final Object first, final Object second) {
            return null == first ? null == second : (null != second && !lt.test(first, second));
        }
    }, regex {
        public boolean test(final Object first, final Object second) {
            return ((String) first).matches((String) second);
        }
    };

    public abstract boolean test(final Object first, final Object second);

    public static Pred valueOf(final Object object) {
        return Pred.valueOf(object.toString());
    }
}
