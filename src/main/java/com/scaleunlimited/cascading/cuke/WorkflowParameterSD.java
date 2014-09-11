package com.scaleunlimited.cascading.cuke;

import java.util.regex.Matcher;

public class WorkflowParameterSD extends BaseStepDefinition {
	public WorkflowParameterSD() {
		super();
		setRegex("the (.+) parameter is (.+)");
	}

	@Override
	public StepDefinition isMatchesStep(String keyword, String description) {
		Matcher matcher = _pattern.matcher(description);
		if (!(matcher.matches())) {
			return null;
		}
		String parameterName = matcher.group(1);
		StringSSE parameterValue = new StringSSE(matcher.group(2));
		_scenarioState.put(parameterName, parameterValue);
		return this;
	}
}

