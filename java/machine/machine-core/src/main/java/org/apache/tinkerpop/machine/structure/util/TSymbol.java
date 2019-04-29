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
package org.apache.tinkerpop.machine.structure.util;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TSymbol {

    private TSymbol() {
        // static instance
    }

    public static final String id = "#id";
    public static final String label = "#label";
    public static final String key = "#key";
    public static final String value = "#value";

    public static boolean isSymbol(final String string) {
        return string.startsWith("#");
    }

    public static String asSymbol(final String string) {
        return string.startsWith("#") ? string : "#" + string;
    }
}
