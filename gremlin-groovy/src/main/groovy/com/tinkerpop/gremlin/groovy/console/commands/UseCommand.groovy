package com.tinkerpop.gremlin.groovy.console.commands

import com.tinkerpop.gremlin.groovy.plugin.Artifact

import java.nio.file.FileSystems
import java.nio.file.StandardCopyOption
import java.nio.file.FileSystem
import com.tinkerpop.gremlin.groovy.console.ConsolePluginAcceptor
import com.tinkerpop.gremlin.groovy.plugin.GremlinPlugin
import groovy.grape.Grape
import org.codehaus.groovy.tools.shell.ComplexCommandSupport
import org.codehaus.groovy.tools.shell.Groovysh

import java.nio.file.Files

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class UseCommand extends ComplexCommandSupport {
    private final Set<String> loadedPlugins

    public UseCommand(final Groovysh shell, final Set<String> loadedPlugins) {
        super(shell, ":use", ":u", ["install", "now"], "now")
        this.loadedPlugins = loadedPlugins
    }

    def Object do_now = { List<String> arguments ->
        final def dep = createDependencyRecord(arguments)
        final def pluginsThatNeedRestart = grabDeps(dep)
        final def msgs = ["loaded: " + arguments]
        if (pluginsThatNeedRestart.size() > 0) {
            msgs << "The following plugins may not function properly unless they are 'installed':"
            msgs.addAll(pluginsThatNeedRestart)
            msgs << "Try :use with the 'install' option then restart the console"
        }

        return msgs
    }

    // todo: add "uninstall" option

    def Object do_install = { List<String> arguments ->
        final def dep = createDependencyRecord(arguments)
        final def pluginsThatNeedRestart = grabDeps(dep)

        final def dependencyLocations = Grape.resolve([classLoader:shell.getInterp().getClassLoader()], null, dep)

        def fileSep = System.getProperty("file.separator")
        def extClassPath = System.getProperty("user.dir") + fileSep + "ext" + fileSep + (String) dep.module

        new File(extClassPath).mkdirs()

        def fs = FileSystems.default
        def target = fs.getPath(extClassPath)

        dependencyLocations.each {
            def from = fs.getPath(it.path)
            Files.copy(from, target.resolve(from.fileName), StandardCopyOption.REPLACE_EXISTING)
        }

        return "loaded: " + arguments + (pluginsThatNeedRestart.size() == 0 ? "" : " - restart the console to use $pluginsThatNeedRestart")
    }

    private def grabDeps(final Map<String, Object> map) {
        Grape.grab(map)

        def pluginsThatNeedRestart = [] as Set
        def additionalDeps = [] as Set

        // note that the service loader utilized the classloader from the groovy shell as shell class are available
        // from within there given loading through Grape.
        ServiceLoader.load(GremlinPlugin.class, shell.getInterp().getClassLoader()).forEach { plugin ->
            if (!loadedPlugins.contains(plugin.name)) {
                if (plugin.requireRestart())
                    pluginsThatNeedRestart<<plugin.name
                else {
                    plugin.pluginTo(new ConsolePluginAcceptor(shell))
                    loadedPlugins.add(plugin.name)
                }

                if (plugin.additionalDependencies().isPresent())
                    additionalDeps.addAll(plugin.additionalDependencies().get().flatten())
            }
        }

        additionalDeps.each { Grape.grab(makeDepsMap((Artifact) it)) }

        return pluginsThatNeedRestart
    }

    private def createDependencyRecord(final List<String> arguments) {
        final String group = arguments.get(0)
        final String module = arguments.get(1)
        final String version = arguments.get(2)

        if (group == null || group.isEmpty())
            throw new IllegalArgumentException("Group cannot be null or empty")

        if (module == null || module.isEmpty())
            throw new IllegalArgumentException("Module cannot be null or empty")

        if (version == null || version.isEmpty())
            throw new IllegalArgumentException("Version cannot be null or empty")

        return makeDepsMap(new Artifact(group, module, version))
    }

    private def makeDepsMap(final Artifact artifact) {
        final Map<String, Object> map = new HashMap<>()
        map.put("classLoader", shell.getInterp().getClassLoader())
        map.put("group", artifact.getGroup())
        map.put("module", artifact.getArtifact())
        map.put("version", artifact.getVersion())
        map.put("changing", false)
        return map
    }
}
