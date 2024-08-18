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
package org.apache.tinkerpop.gremlin.util;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.GValue;

public class ArrayUtil {

    /**
     * Copies the given array and adds the given element at the beginning of the new array.
     * <p>
     * The new array contains the same elements of the input array plus the given element in the first position. The
     * component type of the new array is the same as that of the input array.
     * </p>
     * <p>
     * If the input array is {@code null}, a new one element array is returned whose component type is the same as the
     * element.
     * </p>
     */
    public static GValue[] addFirst(final GValue[] array, final GValue element) {
        final GValue[] elements = new GValue[array.length + 1];
        elements[0] = element;
        System.arraycopy(array, 0, elements, 1, array.length);
        return elements;
    }
}
