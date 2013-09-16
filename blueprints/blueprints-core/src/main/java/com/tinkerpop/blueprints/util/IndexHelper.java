package com.tinkerpop.blueprints.util;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Graph;

import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IndexHelper {

    /**
     * For those graphs that do no support automatic reindexing of elements when a key is provided for indexing, this method can be used to simulate that behavior.
     * The elements in the graph are iterated and their properties (for the provided keys) are removed and then added.
     * Be sure that the key indices have been created prior to calling this method so that they can pick up the property mutations calls.
     * Finally, if the graph is a TransactionalGraph, then a 1000 mutation buffer is used for each commit.
     *
     * @param graph    the graph containing the provided elements
     * @param elements the elements to index into the key indices
     * @param keys     the keys of the key indices
     * @return the number of element properties that were indexed
     */
    public static long reIndexElements(final Graph graph, final Iterable<? extends Element> elements, final Set<String> keys) {
        //final boolean isTransactional = graph instanceof TransactionalGraph;
        long counter = 0;
        for (final Element element : elements) {
            for (final String key : keys) {
                final Object value = element.removeProperty(key);
                if (null != value) {
                    counter++;
                    element.setProperty(key, value);

                    // if (isTransactional && (counter % 1000 == 0)) {
                    //     ((TransactionalGraph) graph).commit();
                    // }
                }
            }
        }
        return counter;
    }
}
