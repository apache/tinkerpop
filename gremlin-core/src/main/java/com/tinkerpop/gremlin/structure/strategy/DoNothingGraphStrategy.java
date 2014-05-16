package com.tinkerpop.gremlin.structure.strategy;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DoNothingGraphStrategy implements GraphStrategy {
	public static final DoNothingGraphStrategy INSTANCE = new DoNothingGraphStrategy();

	private DoNothingGraphStrategy() {}

	@Override
	public String toString() {
		return "passthru";
	}
}
