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
package org.apache.tinkerpop.gremlin.process.traversal.dsl;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * An annotation that specifies that an interface is meant to be used to produce Gremlin DSL. This annotation should
 * be applied to an interface that extends {@link GraphTraversal}. This interface should be suffixed with
 * {@code TraversalDsl}. The DSL classes will be generated to the package of the annotated class or the to the value
 * specified in the {@link #packageName()} and will use the part of the interface name up to the suffix to generate
 * the classes. Therefore, assuming an interface, annotated with {@code GremlinDsl}, called {@code SocialTraversalDsl},
 * there will be three classes generated:
 *
 * <ul>
 *     <li>{@code SocialTraversal} - an interface that is an extension to {@code SocialTraversalDsl}</li>
 *     <li>{@code DefaultSocialTraversal} - an implementation of the {@code SocialTraversal}</li>
 *     <li>{@code SocialTraversalSource} - an extension of {@link GraphTraversalSource} which spawns {@code DefaultSocialTraversal} instances</li>
 * </ul>
 *
 * Together these generated classes provide all the infrastructure required to properly Gremlin traversals enhanced
 * with domain specific steps.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.CLASS)
public @interface GremlinDsl {

    /**
     * The default package name in which to generate the DSL. If this value is left unset or set to an empty string,
     * it will default to the same package as the class or interface the annotation is on.
     */
    public String packageName() default "";

    /**
     * Defines the optional canonical name of the {@link GraphTraversalSource} that this DSL should extend from. If
     * this value is not supplied the generated "source" will simply extend from {@link GraphTraversalSource}.
     */
    public String traversalSource() default "org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource";

    /**
     * When set to {@code true}, which is the default, the following methods will be generated to the DSL
     * implementation of the {@link GraphTraversalSource}:
     *
     * <ul>
     *   <li>{@link GraphTraversalSource#addV()}</li>
     *   <li>{@link GraphTraversalSource#addV(String)}</li>
     *   <li>{@link GraphTraversalSource#V(Object...)}</li>
     *   <li>{@link GraphTraversalSource#E(Object...)}</li>
     * </ul>
     */
    public boolean generateDefaultMethods() default true;
}
