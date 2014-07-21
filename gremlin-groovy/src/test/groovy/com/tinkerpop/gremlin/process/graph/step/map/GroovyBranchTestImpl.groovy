package com.tinkerpop.gremlin.process.graph.step.map

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GroovyBranchTestImpl extends BranchTest {

    public Traversal<Vertex, String> get_g_V_branchXname_length_5XoutXinX_name() {
        return g.V().branch({ it.get().value("name").length() == 5 },
                g.of().out(),
                g.of().in()).name;
    }

    public Traversal<Vertex, String> get_g_v1_branchX0XoutX_name(Object v1Id) {
        return g.v(v1Id).branch({ 0 }, [0: g.of().out().value("name")]);
    }

    public Traversal<Vertex, String> get_g_V_hasXageX_branchXname_lengthX5_in_4_outX_name() {
        return g.V().has("age").branch({ it.get().value("name").length() },
                [4: g.of().out(),
                 5: g.of().in()]).name;
    }

}
