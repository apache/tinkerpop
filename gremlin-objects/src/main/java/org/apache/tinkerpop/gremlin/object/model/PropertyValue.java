/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.object.model;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The {@link PropertyValue} annotation is applied on a unique field of an element class that
 * represents a vertex property, in case it has meta-properties of it's own.
 *
 * <p>
 * Specifically, the field where that element object appears in the vertex class denotes the key
 * of that vertex property. And, the field annotated with {@link PropertyValue} holds the value of
 * that vertex property. The rest of the fields in the (vertex property) element class appear as
 * meta-properties tied to the vertex property.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */

@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface PropertyValue {

}
