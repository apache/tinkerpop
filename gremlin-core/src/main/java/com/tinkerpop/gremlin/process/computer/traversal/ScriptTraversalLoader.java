package com.tinkerpop.gremlin.process.computer.traversal;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ScriptTraversalLoader {

    private static final Map<String, ScriptTraversal> processors = new HashMap<>();

    static {
        ServiceLoader.load(ScriptTraversal.class).forEach(op -> {
            final String name = op.name();

            if (processors.containsKey(name))
                throw new RuntimeException(String.format("There is a naming conflict with the %s ScriptTraversal implementations.", name));

            processors.put(name, op);
        });
    }

    public static Optional<ScriptTraversal> getScriptTraversal(final String name) {
        return Optional.ofNullable(processors.get(name));
    }
}
