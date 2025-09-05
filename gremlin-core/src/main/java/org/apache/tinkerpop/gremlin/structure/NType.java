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
package org.apache.tinkerpop.gremlin.structure;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Number types
 */
public enum NType implements GremlinDataType {
    BYTE(Byte.class),
    SHORT(Short.class),
    INT(int.class),
    INTEGER(Integer.class),
    LONG(Long.class),
    FLOAT(Float.class),
    DOUBLE(Double.class),
    BIG_INTEGER(BigInteger.class),
    BIG_DECIMAL(BigDecimal.class),;

    private final Class<?> javaType;

    NType(Class<?> javaType) {
        this.javaType = javaType;
    }

    @Override
    public String getName() { return this.name(); }

    @Override
    public Class<?> getType() { return javaType; }

    @Override
    public GremlinDataType fromName(String name) { return NType.valueOf(name.toUpperCase()); }

    static {
        // register types
//        for (GremlinDataType value : NType.values()) {
//            GremlinDataType.GlobalTypeCache.registerDataType("NType." + value.getName(), value);
//        }
    }
}
