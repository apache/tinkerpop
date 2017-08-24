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
package org.apache.tinkerpop.gremlin.object.traversal;

import org.apache.tinkerpop.gremlin.object.provider.GraphSystem;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.tinkerpop.gremlin.object.reflect.Classes.name;

/**
 * In order to read objects from the property graph, you'll need an instance of the {@link Query}.
 *
 * <p>
 * Typically, you would create it using an implementation-specific graph factory. However, if the
 * implementation supports it, then it may be dependency inject'ed. The default qualifier is
 * provider by a reference {@code TinkerGraph} system.
 *
 * <p>
 * In general, the following steps are involved in completing a query:
 * <p>
 * <ul>
 *   <li>Specify the traversal you want to apply in the {@link #by} method.</li>
 *   <li>Retrieve a list of objects output by the traversal through the {@link #list} method.</li>
 *   <li>Retrieve a single object output by the traversal through the {@link #one} method.</li>
 * </ul>
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@SuppressWarnings({"rawtypes", "PMD.AvoidDuplicateLiterals"})
public interface Query {

  /**
   * Specify a traversal in terms of a scripted string.
   *
   * @return this {@link Query}
   */
  @SuppressWarnings({"PMD.ShortMethodName"})
  Query by(String traversal);

  /**
   * Specify a traversal as an {@link AnyTraversal} deemed to be complete.
   *
   * @return this {@link Query}
   */
  @SuppressWarnings({"PMD.ShortMethodName"})
  Query by(AnyTraversal traversal);

  /**
   * Specify a chain of {@link SubTraversal}s that will be applied one in order.
   *
   * @return this {@link Query}
   */
  @SuppressWarnings({"PMD.ShortMethodName"})
  Query by(SubTraversal... subTraversals);

  /**
   * Bind a value to a name referenced in the scripted traversal used in {@link #by(String)}.
   *
   * @return this {@link Query}
   */
  Query bind(String name, String value);

  /**
   * Define an alias for the object referenced in a {@link GraphTraversal#select} step.
   *
   * @return this {@link Query}
   */
  @SuppressWarnings({"PMD.ShortMethodName"})
  Query as(String alias, Class elementType);

  /**
   * Set the source of the query to either the vertex ({@link Source#V} or edge ({@link Source#E}}
   * graph.
   *
   * @return this {@link Query}
   */
  Query source(Source source);

  /**
   * Complete the query by retrieving the result as an object of the given element type.
   *
   * Typically, the @{link #type} represents an object element. However, it may also denote the type
   * of a property value.
   *
   * @return an object of the given type
   */
  <T> T one(Class<T> type);

  /**
   * Complete the query by retrieving the result as a list of objects of the given element type.
   *
   * Typically, the @{link #type} represents an object element. However, it may also denote the type
   * of a property value.
   *
   * @return the list of objects of the given type
   */
  <T> List<T> list(Class<T> type);

  /**
   * Complete the traversal specified by the query. This comes in handy when drop steps are
   * involved.
   */
  void none();

  /**
   * Complete the traversal by selecting the aliases specified by {@link Query#as} into their
   * corresponding types of object.
   */
  Selections select();

  /**
   * Return the underlying implementation-specific graph system.
   */
  GraphSystem system();

  /**
   * Release the resources held by the underlying graph system.
   */
  void close();

  /**
   * Disable the specified types of steps.
   */
  void disable(StepType... stepTypes);

  /**
   * Enforce the given mode, by disabling the steps disallowed by it.
   */
  default void enforce(Mode mode) {
    disable(mode.get());
  }

  /**
   * {@link Source} lets you specify whether the starting point of the traversal.
   */
  enum Source implements Supplier<Function<GraphTraversalSource, GraphTraversal>> {
    /**
     * Specifies a traversal of the vertex graph.
     */
    V {
      @Override
      public Function<GraphTraversalSource, GraphTraversal> get() {
        return g -> g.V();
      }
    },
    /**
     * Specifies a traversal of the edge graph.
     */
    E {
      @Override
      public Function<GraphTraversalSource, GraphTraversal> get() {
        return g -> g.E();
      }
    };
  }

  /**
   * {@link Mode} dictates which steps are permitted (or not) by the {@link Query}.
   */
  enum Mode implements Supplier<StepType[]> {
    /**
     * Block steps that can change the graph in this mode.
     */
    READ_ONLY {
      @Override
      public StepType[] get() {
        return new StepType[] {StepType.DROP, StepType.ADDV, StepType.ADDE, StepType.PROPERTY};
      }
    },
    /**
     * Allow both read and write queries in this mode.
     */
    READ_WRITE {
      @Override
      public StepType[] get() {
        return new StepType[] {};
      }
    };
  }

  /**
   * {@link StepType} outlines the steps that the {@link Query} may have to filter for.
   */
  enum StepType {
    DROP, ADDV, ADDE, PROPERTY
  }

  /**
   * Exceptions that might come up during the lifecycle of a query.
   */
  class Exceptions {

    private Exceptions() {}

    public static ClassCastException invalidObjectType(Object element, Class objectClass) {
      return new ClassCastException(
          String.format("The resulting object %s is not of the desired %s type",
              element, name(objectClass)));
    }

    public static IllegalArgumentException unexpectedMultipleResults(Query query) {
      return new IllegalArgumentException(
          String.format("Must specify a traversal that returns a single element: %s", query));
    }

    public static IllegalArgumentException noTraversalSpecified(Query query) {
      return new IllegalArgumentException(
          String.format("Must specify a traversal before completing it: %s", query));
    }

    public static IllegalArgumentException disabledStepQueried(List<StepType> stepTypes) {
      return new IllegalArgumentException(
          String.format("Must specify a traversal without these disabled steps: %s", stepTypes));
    }
  }

}
