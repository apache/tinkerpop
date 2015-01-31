def parse(line, factory) {
    def parts = line.split(/\t/)
    def (id, label, name, x) = parts[0].split(/:/).toList()
    def v1 = factory.vertex(id, label)
    if (name != null) v1.property("name", name)
    if (x != null) {
        if (label.equals("project")) v1.property("lang", x)
        else v1.property("age", Integer.valueOf(x))
    }
    // process out-edges
    if (parts.length >= 2) {
        parts[1].split(/,/).grep { !it.isEmpty() }.each {
            def (eLabel, refId, weight) = it.split(/:/).toList()
            def v2 = factory.vertex(refId)
            def edge = factory.edge(v1, v2, eLabel)
            edge.property("weight", Double.valueOf(weight))
        }
    }
    // process in-edges
    if (parts.length == 3) {
        parts[2].split(/,/).grep { !it.isEmpty() }.each {
            def (eLabel, refId, weight) = it.split(/:/).toList()
            def v2 = factory.vertex(refId)
            def edge = factory.edge(v2, v1, eLabel)
            edge.property("weight", Double.valueOf(weight))
        }
    }
    return v1
}