package com.scaleunlimited.cascading.cuke.stepdefinitions;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.scaleunlimited.cascading.cuke.ScenarioState;
import com.scaleunlimited.cascading.cuke.StepFailureException;

/**
 * A {@link Pattern}-based {@link StepDefinition}
 *
 */
public class BaseStepDefinition implements StepDefinition {
	protected Pattern _pattern;
	protected ScenarioState _scenarioState = null;
	
	public BaseStepDefinition() {
		this(null);
	}
	
	public BaseStepDefinition(Pattern pattern) {
		this(pattern, null);
	}

	public BaseStepDefinition(	Pattern pattern,
								ScenarioState scenarioState) {
		setPattern(pattern);
		setScenarioState(scenarioState);
	}
	
	public void setRegex(String regex) {
		setPattern(Pattern.compile(regex));
	}

	public void setPattern(Pattern pattern) {
		_pattern = pattern;
	}

	public void setScenarioState(ScenarioState scenarioState) {
		_scenarioState = scenarioState;
	}
	
	@Override
	public StepDefinition defineStep(String keyword, String description) {
		Matcher matcher = _pattern.matcher(description);
		return	(	matcher.matches() ?
					this
				:	null);
	}

	@Override
	public void run() throws StepFailureException {
	}

}
