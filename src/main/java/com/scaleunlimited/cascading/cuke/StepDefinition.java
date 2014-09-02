package com.scaleunlimited.cascading.cuke;

/**
 * The implementation of a step.
 * @see BaseStepDefinition.
 */
public interface StepDefinition {
	
	boolean isMatchesStep(String keyword, String description);
	
	void run();
}
