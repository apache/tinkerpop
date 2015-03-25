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
package org.apache.tinkerpop.gremlin.hadoop.process.computer.util;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class Rule implements Serializable {

    public enum Operation {
        OR {
            public Boolean compute(final Object first, final Object second) {
                if (null == first)
                    return (Boolean) second;
                else if (null == second)
                    return (Boolean) first;
                else
                    return (Boolean) first || (Boolean) second;
            }
        }, AND {
            public Boolean compute(final Object first, final Object second) {
                if (null == first)
                    return (Boolean) second;
                else if (null == second)
                    return (Boolean) first;
                else
                    return (Boolean) first && (Boolean) second;
            }
        }, INCR {
            public Long compute(final Object first, final Object second) {
                if (null == first)
                    return (Long) second;
                else if (null == second)
                    return (Long) first;
                else
                    return (Long) first + (Long) second;

            }
        }, SET {
            public Object compute(final Object first, final Object second) {
                return null == first ? second : first;
            }
        }, NO_OP {
            public Object compute(final Object first, final Object second) {
                return null == first ? second : first;
            }
        };

        public abstract Object compute(final Object first, final Object second);
    }

    public final Operation operation;
    public final Object object;

    public Rule(final Operation operation, final Object object) {
        this.operation = operation;
        this.object = object;
    }

    public String toString() {
        return "rule[" + this.operation + ":" + this.object + "]";
    }
}
