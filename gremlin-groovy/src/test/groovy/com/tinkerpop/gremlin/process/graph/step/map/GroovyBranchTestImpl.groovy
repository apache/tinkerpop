package com.tinkerpop.gremlin.process.graph.step.map

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Joshua Shinavier (http://fortytwo.net)
 */
class GroovyBranchTestImpl extends BranchTest {

    public Traversal<Vertex, String> get_g_V_branchXname_length_5XoutXinX_name() {
        return g.V().branch({ it.get().value('name').length() == 5 },
                g.of().out(),
                g.of().in()).name;
    }

    public Traversal<Vertex, String> get_g_v1_branchX0XoutX_name(Object v1Id) {
        return g.v(v1Id).branch({ 0 }, [0: g.of().out().value("name")]);
    }

    public Traversal<Vertex, String> get_g_V_hasXageX_branchXname_lengthX5_in_4_out_3_bothX_name() {
        return g.V().has('age').branch({ it.get().value('name').length() },
                [5: g.of().in(),
                 4: g.of().out(),
                 3: g.of().both()]).name;
    }

    public Traversal<Vertex, Integer> get_g_V_valueXageX_branchXnullX27_identity_29_minus2X() {
        return g.V.age.branch(null, [27:g.of().identity, 29:g.of().map{it.get() - 2}])
    }

    public Traversal<Vertex, Object> get_g_V_branchXout_count_nextX2L_valueXnameX_3L_valuesX() {
        return g.V.branch({it.get().out().count().next();},[
            2L:g.of().value("name"),
            3L:g.of().values()])
    }
}
