package com.tinkerpop.gremlin.groovy.plugin;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Artifact {
    private final String group;
    private final String artifact;
    private final String version;

    public Artifact(final String group, final String artifact, final String version) {
        this.group = group;
        this.artifact = artifact;
        this.version = version;
    }

    public String getGroup() {
        return group;
    }

    public String getArtifact() {
        return artifact;
    }

    public String getVersion() {
        return version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Artifact artifact = (Artifact) o;

        if (group != null ? !group.equals(artifact.group) : artifact.group != null) return false;
        if (artifact != null ? !artifact.equals(artifact.artifact) : artifact.artifact != null) return false;
        if (version != null ? !version.equals(artifact.version) : artifact.version != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = group != null ? group.hashCode() : 0;
        result = 31 * result + (artifact != null ? artifact.hashCode() : 0);
        result = 31 * result + (version != null ? version.hashCode() : 0);
        return result;
    }
}