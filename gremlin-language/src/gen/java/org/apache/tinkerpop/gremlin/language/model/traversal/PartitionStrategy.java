package org.example.org.apache.tinkerpop.gremlin.language.model.traversal;

public abstract class PartitionStrategy {
    private PartitionStrategy() {}
    
    public abstract <R> R accept(Visitor<R> visitor) ;
    
    /**
     * An interface for applying a function to a PartitionStrategy according to its variant (subclass)
     */
    public interface Visitor<R> {
        R visit(IncludeMetaProperties instance) ;
        
        R visit(WritePartition instance) ;
        
        R visit(PartitionKey instance) ;
        
        R visit(ReadPartitions instance) ;
    }
    
    /**
     * An interface for applying a function to a PartitionStrategy according to its variant (subclass). If a visit() method
     * for a particular variant is not implemented, a default method is used instead.
     */
    public interface PartialVisitor<R> extends Visitor<R> {
        default R otherwise(PartitionStrategy instance) {
            throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
        }
        
        default R visit(IncludeMetaProperties instance) {
            return otherwise(instance);
        }
        
        default R visit(WritePartition instance) {
            return otherwise(instance);
        }
        
        default R visit(PartitionKey instance) {
            return otherwise(instance);
        }
        
        default R visit(ReadPartitions instance) {
            return otherwise(instance);
        }
    }
    
    /**
     * @type boolean
     */
    public static final class IncludeMetaProperties extends PartitionStrategy {
        public final Boolean includeMetaProperties;
        
        /**
         * Constructs an immutable IncludeMetaProperties object
         */
        public IncludeMetaProperties(Boolean includeMetaProperties) {
            this.includeMetaProperties = includeMetaProperties;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof IncludeMetaProperties)) return false;
            IncludeMetaProperties o = (IncludeMetaProperties) other;
            return includeMetaProperties.equals(o.includeMetaProperties);
        }
        
        @Override
        public int hashCode() {
            return 2 * includeMetaProperties.hashCode();
        }
    }
    
    /**
     * @type string
     */
    public static final class WritePartition extends PartitionStrategy {
        public final String writePartition;
        
        /**
         * Constructs an immutable WritePartition object
         */
        public WritePartition(String writePartition) {
            this.writePartition = writePartition;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof WritePartition)) return false;
            WritePartition o = (WritePartition) other;
            return writePartition.equals(o.writePartition);
        }
        
        @Override
        public int hashCode() {
            return 2 * writePartition.hashCode();
        }
    }
    
    /**
     * @type string
     */
    public static final class PartitionKey extends PartitionStrategy {
        public final String partitionKey;
        
        /**
         * Constructs an immutable PartitionKey object
         */
        public PartitionKey(String partitionKey) {
            this.partitionKey = partitionKey;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof PartitionKey)) return false;
            PartitionKey o = (PartitionKey) other;
            return partitionKey.equals(o.partitionKey);
        }
        
        @Override
        public int hashCode() {
            return 2 * partitionKey.hashCode();
        }
    }
    
    /**
     * @type list: string
     */
    public static final class ReadPartitions extends PartitionStrategy {
        public final java.util.List<String> readPartitions;
        
        /**
         * Constructs an immutable ReadPartitions object
         */
        public ReadPartitions(java.util.List<String> readPartitions) {
            this.readPartitions = readPartitions;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ReadPartitions)) return false;
            ReadPartitions o = (ReadPartitions) other;
            return readPartitions.equals(o.readPartitions);
        }
        
        @Override
        public int hashCode() {
            return 2 * readPartitions.hashCode();
        }
    }
}
