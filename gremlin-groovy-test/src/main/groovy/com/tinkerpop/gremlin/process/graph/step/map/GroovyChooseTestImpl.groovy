package com.tinkerpop.gremlin.process.graph.step.map

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public abstract class GroovyChooseTestImpl {

    public static class StandardTestImpl extends ChooseTest {

        @Override
        public Traversal<Vertex, String> get_g_V_chooseXname_length_5XoutXinX_name() {
            return g.V.choose({ it.name.length() == 5 },
                    g.of().out(),
                    g.of().in()).name;
        }

        @Override
        public Traversal<Vertex, String> get_g_v1_chooseX0XoutX_name(Object v1Id) {
            return g.v(v1Id).choose({ 0 }, [0: g.of().out.name]);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasXageX_chooseXname_lengthX5_in_4_out_3_bothX_name() {
            return g.V().has('age').choose({ it.name.length() },
                    [5: g.of().in(),
                     4: g.of().out(),
                     3: g.of().both()]).name;
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_chooseXout_count_nextX2L_valueXnameX_3L_valueMapX() {
            return g.V().choose({ it.out().count().next(); }, [
                    2L: g.of().name,
                    3L: g.of().valueMap()])
        }
    }
}
