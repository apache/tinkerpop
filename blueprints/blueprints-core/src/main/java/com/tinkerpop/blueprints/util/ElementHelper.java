package com.tinkerpop.blueprints.util;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

import java.util.Objects;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ElementHelper {

    /**
     * A standard method for determining if two elements are equal.
     * This method should be used by any Element.equals() implementation to ensure consistent behavior.
     *
     * @param a The first element
     * @param b The second element (as an object)
     * @return Whether the two elements are equal
     */
    public static boolean areEqual(final Element a, final Object b) {
        Objects.requireNonNull(a);
        Objects.requireNonNull(b);
        if (a == b)
            return true;
        if (!((a instanceof Vertex && b instanceof Vertex) || (a instanceof Edge && b instanceof Edge)))
            return false;
        return a.getId().equals(((Element) b).getId());
    }

    /**
     * Simply tests if the element ids are equal().
     *
     * @param a the first element
     * @param b the second element
     * @return Whether the two elements have equal ids
     */
    public static boolean haveEqualIds(final Element a, final Element b) {
        return a.getId().equals(b.getId());
    }


}
