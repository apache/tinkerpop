package com.tinkerpop.gremlin.groovy.util

import com.tinkerpop.gremlin.groovy.plugin.Artifact
import groovy.grape.Grape

import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.StandardCopyOption

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class DependencyGrabber {

    private final ClassLoader classLoaderToUse
    private final String extensionDirectory

    public DependencyGrabber(final ClassLoader cl, final String extensionDirectory) {
        this.classLoaderToUse = cl
        this.extensionDirectory = extensionDirectory
    }

    def void copyDependenciesToPath(final Artifact artifact) {
        final def dep = makeDepsMap(artifact)
        final String extClassPath = getPathFromDependency(dep)
        final File f = new File(extClassPath)

        if (f.exists()) throw new IllegalStateException("a module with the name ${dep.module} is already installed")
        if (!f.mkdirs()) throw new IOException("could not create directory at ${f}")

        final def dependencyLocations = Grape.resolve([classLoader: this.classLoaderToUse], null, dep)
        def fs = FileSystems.default
        def target = fs.getPath(extClassPath)

        // ignore slf4j related jars.  they are already in the path and will create duplicate bindings which
        // generate annoying log messages that make you think stuff is wrong
        dependencyLocations.collect{fs.getPath(it.path)}
                .findAll{!(it.fileName.toFile().name ==~ /(slf4j|logback\-classic)-.*\.jar/)}
                .each { Files.copy(it, target.resolve(it.fileName), StandardCopyOption.REPLACE_EXISTING) }
    }

    def void copyDependenciesToPath(final String group, final String artifact, final String version) {
        copyDependenciesToPath(new Artifact(group, artifact, version))
    }

    private String getPathFromDependency(final Map<String, Object> dep) {
        def fileSep = System.getProperty("file.separator")
        return this.extensionDirectory + fileSep + (String) dep.module
    }

    private def makeDepsMap(final Artifact artifact) {
        final Map<String, Object> map = new HashMap<>()
        map.put("classLoader", this.classLoaderToUse)
        map.put("group", artifact.getGroup())
        map.put("module", artifact.getArtifact())
        map.put("version", artifact.getVersion())
        map.put("changing", false)
        return map
    }
}
