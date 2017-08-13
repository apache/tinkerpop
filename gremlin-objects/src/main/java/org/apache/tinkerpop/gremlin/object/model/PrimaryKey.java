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

import org.apache.tinkerpop.gremlin.object.traversal.library.HasKeys;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A field annotated with {@link PrimaryKey} represents a property in the underlying graph element
 * by which identification should occur. Ideally, the set of fields with this annotation should be a
 * minimal superkey, which is both necessary and sufficient to identity the element.
 *
 * <p>
 * If the graph supports user supplied ids, then such fields will be included in the id
 * generated when objects are created in the graph. If not, then such fields help us find objects in
 * the graph whose ids are unknown, through the {@link HasKeys} sub-traversal.
 *
 * <p>
 * While most objects have one primary key field, they may have more than one. To facilitate
 * fast lookups, there must be at least one primary key field, especially if element ids are not
 * user supplied, in which case we may not always have those ids handy.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */

@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface PrimaryKey {

}
