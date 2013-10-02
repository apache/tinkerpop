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

    public boolean isSimple() {
        return !(new LinkedHashSet(this).size() == this.size());
    }
}
