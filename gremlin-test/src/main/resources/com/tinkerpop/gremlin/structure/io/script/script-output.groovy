def stringify(vertex) {
    def edgeMap = { vdir ->
        return {
            def e = it.get()
            def v = e."${vdir}V"().next()
            e.values("weight").inject(e.label(), v.label(), v.id()).join(":")
        }
    }
    def v = vertex.values("name", "age", "lang").inject(vertex.id(), vertex.label()).join(":")
    def outE = vertex.outE().map(edgeMap("in")).join(",")
    def inE = vertex.inE().map(edgeMap("out")).join(",")
    return [v, outE, inE].join("\t")
}