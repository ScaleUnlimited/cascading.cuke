package com.scaleunlimited.cascading.cuke;

import java.util.regex.Matcher;

public class WorkflowCounterAssertionSD extends BaseStepDefinition {
	public WorkflowCounterAssertionSD() {
		super();
		setRegex("the (.+) counter value is (.+)");
	}

	@Override
	public boolean isMatchesStep(String keyword, String description) {
		Matcher matcher = _pattern.matcher(description);
		boolean result = matcher.matches();
		if (result) {
			String counterKey = matcher.group(1);
			String counterValueAssertion = matcher.group(2);
			WorkflowCounterAssertionSSE assertion =
				new WorkflowCounterAssertionSSE(counterKey, 
												counterValueAssertion);
			assertion.addToScenario(_scenarioState);
		}
		return result;
	}
}
