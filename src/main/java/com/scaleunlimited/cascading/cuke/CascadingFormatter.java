package com.scaleunlimited.cascading.cuke;

import gherkin.formatter.PrettyFormatter;
import gherkin.formatter.model.Background;
import gherkin.formatter.model.DocString;
import gherkin.formatter.model.Feature;
import gherkin.formatter.model.Result;
import gherkin.formatter.model.Scenario;
import gherkin.formatter.model.Step;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO delegate to PrettyFormatter?
public class CascadingFormatter extends PrettyFormatter {
    static final Logger LOGGER = LoggerFactory.getLogger(CascadingFormatter.class);

    private Feature _currentFeature = null;
    private Background _currentBackground = null;
    private Scenario _currentScenario = null;
    private boolean _isRecordingBackground = false;
    private List<Step> _backgroundSteps = null;
    private ScenarioState _scenarioState = null;
    private List<StepDefinition> _backgroundStepDefinitions = null;
    private List<StepDefinition> _scenarioStepDefinitions = null;
	
    private List<StepDefinition> _stepDefinitions = null;

	public CascadingFormatter(	Appendable out,
								boolean monochrome,
								boolean executing) {
		super(out, monochrome, executing);
		_backgroundSteps = new ArrayList<Step>();
		_scenarioState = new ScenarioState();
		_backgroundStepDefinitions = new ArrayList<StepDefinition>();
		_scenarioStepDefinitions = new ArrayList<StepDefinition>();
		_stepDefinitions = new ArrayList<StepDefinition>();
		
		// TODO Will there be any default StepDefinitions?
	}
	
	public void addStepDefinition(BaseStepDefinition stepDefinition) {
		stepDefinition.setScenarioState(_scenarioState);
		_stepDefinitions.add(stepDefinition);
	}

	@Override
	public void feature(Feature feature) {
		super.feature(feature);
		resetScenarioState();
		_currentFeature = feature;
		_currentScenario = null;
		_currentBackground = null;
		_backgroundSteps.clear();
		if (_isRecordingBackground) {
			_isRecordingBackground = false;
			LOGGER.warn("New feature before background complete (i.e., previous feature has background, but no scenarios.)");
		}
	}

	// TODO Can there be multiple backgrounds?
	// TODO Can a background follow a scenario in a feature file?
	@Override
	public void background(Background background) {
		super.background(background);
		if (_currentFeature == null) {
			throw new IllegalStateException("Background encountered outside scope of any feature");
		}
		_currentBackground = background;
		_isRecordingBackground = true;
	}

	@Override
	public void scenario(Scenario scenario) {
		_isRecordingBackground = false;
		runCurrentScenario();
		super.scenario(scenario);
		if (_currentFeature == null) {
			throw new IllegalStateException("Scenario encountered outside scope of any feature");
		}
		_currentScenario = scenario;
		for (Step backgroundStep : _backgroundSteps) {
			_backgroundStepDefinitions.add(defineStep(backgroundStep));
		}
	}

	@Override
	public void step(Step step) {
		super.step(step);
		if (_isRecordingBackground) {
			_backgroundSteps.add(step);
		} else {
			if (_currentScenario == null) {
				throw new IllegalStateException("Step encountered outside scope of any background or scenario");
			}
			_scenarioStepDefinitions.add(defineStep(step));
		}
	}
	
	private StepDefinition defineStep(Step step) {
		StepDefinition result = null;
		for (StepDefinition stepDefinition : _stepDefinitions) {
			StepDefinition matchingStepDefinition = 
				stepDefinition.defineStep(step.getKeyword(), step.getName());
			if (matchingStepDefinition != null) {
				if (result == null) {
					result = matchingStepDefinition;
				} else {
					// TODO Am I supposed to call syntaxError here?
					String message = 
						String.format(	"Multiple step definitions match %s (e.g., both %s and %s)",
										getStepDescription(step),
										result,
										stepDefinition);
					throw new RuntimeException(message);
				}
			}
		}

		return result;
	}
	
	@Override
	public void eof() {
		runCurrentScenario();
		super.eof();
	}
	
	private static String getStepDescription(Step step) {
		String result = null;
		DocString docString = step.getDocString();
		if (docString != null) {
			result = docString.toString();
		}
		if (result == null) {
			result = String.format(	"'%s%s' from line %d",
									step.getKeyword(),
									step.getName(),
									step.getLine());
		}
		return result;
	}

	private void resetScenarioState() {
		try {
			_scenarioState.clear();
		} catch (IOException e) {
			LOGGER.error("Error closing scenario state", e);
		}
		_backgroundStepDefinitions.clear();
		_scenarioStepDefinitions.clear();
	}
	
	private void runCurrentScenario() {
		if (_currentBackground != null) {
			runStepDefinitions(_backgroundStepDefinitions);
		}
		if (_currentScenario != null) {
			runStepDefinitions(_scenarioStepDefinitions);
		}
		resetScenarioState();
	}

	private void runStepDefinitions(List<StepDefinition> stepDefinitions) {
		for (StepDefinition stepDefinition : stepDefinitions) {
			if (stepDefinition == null) {
				result(Result.UNDEFINED);
			} else {
				try {
					stepDefinition.run();
					result(new Result(Result.PASSED, 0L, null, null));
				} catch (StepFailureException e) {
					result(new Result(Result.FAILED, 0L, e, null));
				}
			}
		}
	}
}
