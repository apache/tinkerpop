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
package org.apache.tinkerpop.machine.processor;

import org.apache.tinkerpop.machine.traverser.Traverser;
import org.apache.tinkerpop.machine.util.IteratorUtils;

import java.util.Iterator;
import java.util.function.Consumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Processor<C, S, E> {

    /**
     * A processor is started using {@link Processor#iterator(Iterator)} or {@link Processor#subscribe(Iterator, Consumer)}.
     * When the iterator is empty or the subscriber has no more traversers to consume, then the process is no longer running.
     *
     * @return whether the processor is running
     */
    public boolean isRunning();

    /**
     * When a processor is stopped, subscriptions and iteration are halted.
     */
    public void stop();

    /**
     * Start the processor and return a pull-based iterator.
     * If pull-based iteration is used, then push-based subscription can not be used while the processor is running.
     *
     * @return an iterator of traverser results
     */
    public Iterator<Traverser<C, E>> iterator(final Iterator<Traverser<C, S>> starts);

    public default Iterator<Traverser<C, E>> iterator(final Traverser<C, S> start) {
        return this.iterator(IteratorUtils.of(start));
    }

    /**
     * Start the processor and process the resultant traversers using the push-based consumer.
     * If push-based subscription is used, then pull-based iteration can not be used while the processor is running.
     *
     * @param consumer a consumer of traversers results
     */
    public void subscribe(final Iterator<Traverser<C, S>> starts, final Consumer<Traverser<C, E>> consumer);

    public class Exceptions {

        public static IllegalStateException processorIsCurrentlyRunning(final Processor processor) {
            return new IllegalStateException("The processor can not be started because it is currently running: " + processor);
        }
    }

}
