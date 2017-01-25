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

package org.apache.tinkerpop.gremlin.util;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ClassUtil {

    private ClassUtil() {
        // static method class
    }

    public static String getClassName(final Class<?> clazz) {
        if (null == clazz.getEnclosingClass())
            return clazz.getCanonicalName();
        else {
            if (clazz.getEnclosingClass().isEnum())
                return clazz.getName().replace("$", "#");
            else {
                final String className = clazz.getCanonicalName();
                int index = className.lastIndexOf(".");
                return className.substring(0, index) + "$" + className.substring(index + 1);
            }
        }
    }

    public static <V> V getClassOrEnum(final String name) {
        try {
            if (name.contains("#")) { // this is an enum
                String[] names = name.split("#");
                return (V) Class.forName(names[0]).getEnumConstants()[Integer.valueOf(names[1]) - 1];
            } else {
                return (V) Class.forName(name);
            }
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
