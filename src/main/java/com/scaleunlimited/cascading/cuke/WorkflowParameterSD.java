package com.scaleunlimited.cascading.cuke;

import java.util.regex.Matcher;

public class WorkflowParameterSD extends BaseStepDefinition {
	public WorkflowParameterSD() {
		super();
		setRegex("the (.+) parameter is (.+)");
	}

	@Override
	public StepDefinition defineStep(String keyword, String description) {
		Matcher matcher = _pattern.matcher(description);
		if (!(matcher.matches())) {
			return null;
		}
		String parameterName = matcher.group(1);
		SimpleSSE<String> parameterValue = 
			new SimpleSSE<String>(matcher.group(2));
		_scenarioState.put(parameterName, parameterValue);
		return this;
	}
}

