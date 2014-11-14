package com.tinkerpop.gremlin.console.commands

import com.tinkerpop.gremlin.console.Mediator
import com.tinkerpop.gremlin.console.plugin.PluggedIn
import com.tinkerpop.gremlin.groovy.plugin.Artifact
import com.tinkerpop.gremlin.groovy.plugin.GremlinPlugin
import groovy.grape.Grape
import org.codehaus.groovy.tools.shell.CommandSupport
import org.codehaus.groovy.tools.shell.Groovysh

import java.nio.file.DirectoryStream
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption

/**
 * Install a dependency into the console.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class InstallCommand extends CommandSupport {

    private final static String fileSep = System.getProperty("file.separator")
    private final Mediator mediator

    public InstallCommand(final Groovysh shell, final Mediator mediator) {
        super(shell, ":install", ":+")
        this.mediator = mediator
    }

    @Override
    def Object execute(final List<String> arguments) {
        final def dep = createDependencyRecord(arguments)
        final def pluginsThatNeedRestart = grabDeps(dep)

        final String extClassPath = getPathFromDependency(dep)
        final File f = new File(extClassPath)
        if (f.exists())
            return "A module with the name ${dep.module} is already installed"
        else
            f.mkdirs()

        final def dependencyLocations = Grape.resolve([classLoader: shell.getInterp().getClassLoader()], null, dep)

        def fs = FileSystems.default
        def target = fs.getPath(extClassPath)

        // collect the files already on the path in /lib
        def filesAlreadyInPath = []
        getFileNames(filesAlreadyInPath, fs.getPath(System.getProperty("user.dir") + fileSep + "lib"))

        // ignore slf4j related jars.  they are already in the path and will create duplicate bindings which
        // generate annoying log messages that make you think stuff is wrong.  also, don't bring over files
        // that are already on the path
        dependencyLocations.collect{fs.getPath(it.path)}
                .findAll{!(it.fileName.toFile().name ==~ /(slf4j|logback\-classic)-.*\.jar/)}
                .findAll{!filesAlreadyInPath.collect{it.getFileName().toString()}.contains(it.fileName.toFile().name)}
                .each { Files.copy(it, target.resolve(it.fileName), StandardCopyOption.REPLACE_EXISTING) }

        return "Loaded: " + arguments + (pluginsThatNeedRestart.size() == 0 ? "" : " - restart the console to use $pluginsThatNeedRestart")
    }

    private static String getPathFromDependency(final Map<String, Object> dep) {
        def extClassPath = System.getProperty("user.dir") + fileSep + "ext" + fileSep + (String) dep.module
        return extClassPath
    }

    private def grabDeps(final Map<String, Object> map) {
        Grape.grab(map)

        def pluginsThatNeedRestart = [] as Set
        def additionalDeps = [] as Set

        // note that the service loader utilized the classloader from the groovy shell as shell class are available
        // from within there given loading through Grape.
        ServiceLoader.load(GremlinPlugin.class, shell.getInterp().getClassLoader()).forEach { plugin ->
            if (!mediator.availablePlugins.containsKey(plugin.class.name)) {
                mediator.availablePlugins.put(plugin.class.name, new PluggedIn(plugin, shell, io, false))
                if (plugin.requireRestart())
                    pluginsThatNeedRestart << plugin.name

                if (plugin.additionalDependencies().isPresent())
                    additionalDeps.addAll(plugin.additionalDependencies().get().flatten())
            }
        }

        additionalDeps.each { Grape.grab(makeDepsMap((Artifact) it)) }

        return pluginsThatNeedRestart
    }

    private def createDependencyRecord(final List<String> arguments) {
        final String group = arguments.size() >= 1 ? arguments.get(0) : null
        final String module = arguments.size() >= 2 ? arguments.get(1) : null
        final String version = arguments.size() >=3 ? arguments.get(2) : null

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

    private static void getFileNames(final List fileNames, final Path dir){
        try {
            final DirectoryStream<Path> stream = Files.newDirectoryStream(dir);
            for (Path path : stream) {
                if(path.toFile().isDirectory()) getFileNames(fileNames, path);
                else {
                    fileNames.add(path.toAbsolutePath());
                }
            }
            stream.close();
        } catch(IOException e){
            e.printStackTrace();
        }
    }
}