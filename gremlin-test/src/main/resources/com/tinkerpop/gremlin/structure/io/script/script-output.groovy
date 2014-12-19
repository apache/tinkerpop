def stringify(vertex) {
    def edgeMap = { vdir ->
        return {
            def e = it.get()
            e.values("weight").inject(e.label(), e."${vdir}V"().next().id()).join(":")
        }
    }
    def v = vertex.values("name", "age", "lang").inject(vertex.id(), vertex.label()).join(":")
    def outE = vertex.outE().map(edgeMap("in")).join(",")
    def inE = vertex.inE().map(edgeMap("out")).join(",")
    return [v, outE, inE].join("\t")
}