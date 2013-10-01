package com.tinkerpop.gremlin.pipes.util;

import java.util.ArrayList;
import java.util.LinkedHashSet;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Path extends ArrayList {

    public Path() {
        super();
    }

    public Path(final Path path) {
        super(path);
    }

    public boolean add(final Object object) {
        if (this.size() == 0)
            return super.add(object);
        else if (!object.equals(this.get(this.size() - 1)))
            return super.add(object);
        else
            return false;
    }

    public boolean isSimple() {
        return !(new LinkedHashSet(this).size() == this.size());
    }
}
