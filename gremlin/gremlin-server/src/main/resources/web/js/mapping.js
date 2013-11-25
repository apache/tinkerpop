tinkerpop.namespace("tinkerpop.mapping");
tinkerpop.mapping = (function () {
    return {
        arg: {
            coordinates: "coordinates",
            gremlin: "gremlin",
            imports: "imports",
            infoType: "infoType",
            verbose: "verbose"
        },

        coordinates: {
            artifact: "artifact",
            group: "group",
            version: "version"
        },

        infoType: {
            dependencies: "dependencies",
            imports: "imports",
            variables: "variables"
        },

        op: {
            cancel: "cancel",
            eval: "eval",
            import: "import",
            reset: "reset",
            show: "show",
            use: "use",
            version: "version"
        }
    };
}());