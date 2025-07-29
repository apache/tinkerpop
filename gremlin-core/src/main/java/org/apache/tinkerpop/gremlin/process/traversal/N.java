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
package org.apache.tinkerpop.gremlin.process.traversal;

import org.apache.tinkerpop.gremlin.process.traversal.step.map.AsNumberStep;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Tokens that are used to denote different units of number.
 * Used with {@link AsNumberStep} step.
 */
public enum N {
    byte_(Byte.class),
    short_(Short.class),
    int_(Integer.class),
    long_(Long.class),
    float_(Float.class),
    double_(Double.class),
    bigInt(BigInteger.class),
    bigDecimal(BigDecimal.class),;

    private final Class<? extends Number> type;

    N(Class<? extends Number> type) {this.type = type;}

    public Class<? extends Number> getType() {
        return this.type;
    }

}