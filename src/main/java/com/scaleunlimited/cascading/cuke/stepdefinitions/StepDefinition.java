package com.scaleunlimited.cascading.cuke.stepdefinitions;

import com.scaleunlimited.cascading.cuke.StepFailureException;

/**
 * The implementation of a step.
 * @see BaseStepDefinition.
 */
public interface StepDefinition {
	
	/**
	 * @param keyword introducing the step (typically used for reporting only)
	 * @param description of the step to match against this step definition
	 * @return a matching {@link StepDefinition} instance
	 * (though not necessarily this one, as it may incorporate state harvested
	 * from the <code>description</code>), or null if the step doesn't match.
	 */
	StepDefinition defineStep(String keyword, String description);
	
	/**
	 * Execute the step definition.  These methods should be called in the
	 * same order as the steps were found in the input file, but after all of
	 * the steps in the scenario have first been matched.
	 * @throws StepFailureException
	 */
	void run() throws StepFailureException;
}
