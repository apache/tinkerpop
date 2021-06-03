package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class ProjectStep {
    /**
     * @type string
     */
    public final String projectKey;
    
    /**
     * @type list: string
     */
    public final java.util.List<String> otherProjectKeys;
    
    /**
     * Constructs an immutable ProjectStep object
     */
    public ProjectStep(String projectKey, java.util.List<String> otherProjectKeys) {
        this.projectKey = projectKey;
        this.otherProjectKeys = otherProjectKeys;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ProjectStep)) {
            return false;
        }
        ProjectStep o = (ProjectStep) other;
        return projectKey.equals(o.projectKey)
            && otherProjectKeys.equals(o.otherProjectKeys);
    }
    
    @Override
    public int hashCode() {
        return 2 * projectKey.hashCode()
            + 3 * otherProjectKeys.hashCode();
    }
    
    /**
     * Construct a new immutable ProjectStep object in which projectKey is overridden
     */
    public ProjectStep withProjectKey(String projectKey) {
        return new ProjectStep(projectKey, otherProjectKeys);
    }
    
    /**
     * Construct a new immutable ProjectStep object in which otherProjectKeys is overridden
     */
    public ProjectStep withOtherProjectKeys(java.util.List<String> otherProjectKeys) {
        return new ProjectStep(projectKey, otherProjectKeys);
    }
}
