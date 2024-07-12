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
package org.apache.tinkerpop.gremlin.process.traversal.step;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An enum that describes types that are used in the Gremlin language.
 */
public enum GType {
    BIG_DECIMAL,
    BIG_INTEGER,
    BOOLEAN,
    DOUBLE,
    EDGE,
    INTEGER,
    LIST,
    LONG,
    MAP,
    PATH,
    PROPERTY,
    SET,
    STRING,
    UNKNOWN,
    VERTEX;

    GType() {}

    /**
     * Returns true if the type is a number.
     */
    public boolean isNumeric() {
        return this == INTEGER || this == DOUBLE || this == LONG || this == BIG_INTEGER || this == BIG_DECIMAL;
    }

    /**
     * Convert an object to a matching {@link GType} and if not matched return {@link GType#UNKNOWN}.
     */
    public static GType getType(final Object object) {
        if (object instanceof String) return STRING;
        else if (object instanceof Integer) return INTEGER;
        else if (object instanceof Boolean) return BOOLEAN;
        else if (object instanceof Double) return DOUBLE;
        else if (object instanceof Long) return LONG;
        else if (object instanceof Map) return MAP;
        else if (object instanceof List) return LIST;
        else if (object instanceof Set) return SET;
        else if (object instanceof Vertex) return VERTEX;
        else if (object instanceof Edge) return EDGE;
        else if (object instanceof Path) return PATH;
        else if (object instanceof Property) return PROPERTY;
        else if (object instanceof BigInteger) return BIG_INTEGER;
        else if (object instanceof BigDecimal) return BIG_DECIMAL;
        else return UNKNOWN;
    }
}
