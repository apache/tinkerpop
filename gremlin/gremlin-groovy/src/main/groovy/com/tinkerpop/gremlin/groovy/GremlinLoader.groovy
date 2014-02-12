package com.tinkerpop.gremlin.groovy

import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine
import com.tinkerpop.gremlin.groovy.loaders.GraphLoader
import com.tinkerpop.gremlin.groovy.loaders.PipeLoader
import com.tinkerpop.gremlin.process.Step
import com.tinkerpop.gremlin.process.Tokens
import com.tinkerpop.gremlin.process.Traversal
import groovy.grape.Grape

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GremlinLoader {

    private static final Set<String> steps = new HashSet<String>()
    private static final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine()

    /**
     * The list of loaded plugins for the console.
     */
    private static final Set<String> loadedPlugins = new HashSet<String>()

    public static void load() {

        Traversal.getMethods().each {
            if (it.getReturnType().equals(Pipeline.class)) {
                addStep(it.getName())
            }
        }

        //ElementLoader.load()
        GraphLoader.load()
        //IndexLoader.load()
        //ObjectLoader.load()
        PipeLoader.load()

        try {
            //SailGraphLoader.load()
        } catch (Throwable e) {
            // this means that SailGraph was not in the dependency
            // that is ok
        }
    }

    /*private static GremlinGroovyPipeline compose(final Object start, final Step step) {
        GremlinGroovyPipeline pipeline
        if (start instanceof GremlinGroovyPipeline) {
            pipeline = start
            if (null != step)
                pipeline.addStep(step)
        } else if (start instanceof Step) {
            pipeline = new GremlinGroovyPipeline()
            pipeline.addStep(start)
            if (null != step)
                pipeline.addStep(step)
        } else {
            pipeline = new GremlinGroovyPipeline(start)
            if (null != step)
                pipeline.addStep(step)
        }

        return pipeline
    }*/

    public static Step compile(final String script) {
        return (Step) engine.eval(script, engine.createBindings())
    }

    public static void addStep(final String stepName) {
        steps.add(stepName)
    }

    public static boolean isStep(final String stepName) {
        return steps.contains(stepName)
    }

    public static Set<String> getStepNames() {
        return new HashSet(steps)
    }

    public static void defineStep(final String stepName, final List<Class> classes, final Closure stepClosure) {
        GremlinLoader.steps.add(stepName);
        classes.each {
            stepClosure.setDelegate(delegate);
            it.metaClass."$stepName" = { final Object... parameters ->
                GremlinLoader.compose(delegate, stepClosure(* parameters))
            };
        }
    }

    public static String version() {
        return Tokens.VERSION
    }

    public static String language() {
        return "gremlin-groovy"
    }

    public static Set plugins() {
        return loadedPlugins
    }

    public static def deps() {
        return Grape.listDependencies(com.tinkerpop.gremlin.groovy.console.Console.groovysh.interp.classLoader).sort { a, b -> a.key <=> b.key }
    }

    /*public static void use(final Map dependency) {
        if (dependency == null || dependency.isEmpty())
            throw IllegalArgumentException("dependency cannot be null or empty")

        // note that the groovysh classloader has to be passed to grape so that the classes get loaded inside the shell.
        // mileage may vary on the ":changing" argument...depends on the user's repo configuration and resolver.
        // not super-clear on how to get the environment right for it to work but there was some explanation here:
        // http://groovy.329449.n5.nabble.com/How-to-tell-Grape-to-refresh-updated-snapshots-td355460.html
        Grape.grab([classLoader:com.tinkerpop.gremlin.groovy.console.Console.groovysh.interp.classLoader],
                [group:dependency.group, module:dependency.module, version:dependency.version,
                        changing: dependency.changing])

        // note that the service loader utilized the classloader from the groovy shell as shell class are available
        // from within there given loading through Grape.
        ServiceLoader.load(ConsolePlugin.class, com.tinkerpop.gremlin.groovy.console.Console.groovysh.interp.classLoader).each{
            if (!Gremlin.plugins().contains(it.name)) {
                it.pluginTo(new ConsoleGroovy(com.tinkerpop.gremlin.groovy.console.Console.groovysh), new ConsoleIO(it, com.tinkerpop.gremlin.groovy.console.Console.standardIo));
                Gremlin.plugins().add(it.name)
                com.tinkerpop.gremlin.groovy.console.Console.standardIo.out.println(com.tinkerpop.gremlin.groovy.console.Console.STANDARD_RESULT_PROMPT + "Plugin Loaded: " + it.name)
            }
        }
    }

    public static void use(final String group, final String module, final String version, final boolean changing = false) {
        if (group == null || group.isEmpty())
            throw IllegalArgumentException("group cannot be null or empty")

        if (module == null || module.isEmpty())
            throw IllegalArgumentException("module cannot be null or empty")

        if (version == null || version.isEmpty())
            throw IllegalArgumentException("version cannot be null or empty")
        use([group:group, module:module, version:version, changing:changing])
    }*/
}
