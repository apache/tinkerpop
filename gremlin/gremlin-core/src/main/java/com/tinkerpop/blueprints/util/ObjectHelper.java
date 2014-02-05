package com.tinkerpop.blueprints.util;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ObjectHelper {

    public static boolean contains(final Object object, final Object... objects) {
        for (int i = 0; i < objects.length; i++) {
            if (object.equals(objects[i]))
                return true;
        }
        return false;
    }
}
