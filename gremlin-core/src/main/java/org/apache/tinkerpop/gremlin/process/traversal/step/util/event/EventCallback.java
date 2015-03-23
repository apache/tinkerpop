package org.apache.tinkerpop.gremlin.process.traversal.step.util.event;

import java.util.function.Consumer;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@FunctionalInterface
public interface EventCallback<E extends Event> extends Consumer<E> {
}
