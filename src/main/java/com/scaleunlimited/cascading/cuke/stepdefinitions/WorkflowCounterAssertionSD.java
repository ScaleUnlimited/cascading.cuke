package com.scaleunlimited.cascading.cuke.stepdefinitions;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.scaleunlimited.cascading.FlowResult;
import com.scaleunlimited.cascading.cuke.SimpleSSE;
import com.scaleunlimited.cascading.cuke.StepFailureException;

public class WorkflowCounterAssertionSD extends BaseStepDefinition {
	private static final Pattern EQUALS_PATTERN = Pattern.compile("^([0-9]+)$");
	private static final Pattern LESS_THAN_PATTERN = Pattern.compile("^(?:less than|<)\\s*([0-9]+)$");
	private static final Pattern AT_MOST_PATTERN = Pattern.compile("^(?:at most|<=)\\s*([0-9]+)$");
	private static final Pattern AT_LEAST_PATTERN = Pattern.compile("^(?:at least|>=)\\s*([0-9]+)$");
	private static final Pattern GREATER_THAN_PATTERN = Pattern.compile("^(?:greater than|more than|>)\\s*([0-9]+)$");
	
	private static interface Assertion {
		public boolean isSatisfied(long counterValue);
	}
	
	private static class EqualsAssertion implements Assertion {
		long _expectedValue;
		
		public EqualsAssertion(long expectedValue) {
			_expectedValue = expectedValue;
		}

		@Override
		public boolean isSatisfied(long counterValue) {
			return (counterValue == _expectedValue);
		}
	}
	
	private static class LessThanAssertion implements Assertion {
		long _minInvalidValue;
		
		public LessThanAssertion(long minInvalidValue) {
			_minInvalidValue = minInvalidValue;
		}

		@Override
		public boolean isSatisfied(long counterValue) {
			return (counterValue < _minInvalidValue);
		}
	}
	
	private static class AtMostAssertion implements Assertion {
		long _maxValue;
		
		public AtMostAssertion(long maxValue) {
			_maxValue = maxValue;
		}

		@Override
		public boolean isSatisfied(long counterValue) {
			return (counterValue <= _maxValue);
		}
	}
	
	private static class AtLeastAssertion implements Assertion {
		long _minValue;
		
		public AtLeastAssertion(long minValue) {
			_minValue = minValue;
		}

		@Override
		public boolean isSatisfied(long counterValue) {
			return (counterValue >= _minValue);
		}
	}
	
	private static class GreaterThanAssertion implements Assertion {
		long _maxInvalidValue;
		
		public GreaterThanAssertion(long maxInvalidValue) {
			_maxInvalidValue = maxInvalidValue;
		}

		@Override
		public boolean isSatisfied(long counterValue) {
			return (counterValue > _maxInvalidValue);
		}
	}
	
	private String _workflowName;
	private String _counterKey;
	private String _assertionDescription;
	private Assertion _assertion;

	public WorkflowCounterAssertionSD() {
		super();
		setRegex("the (.+) (.+) counter value is (.+)");
	}
	
	@Override
	public StepDefinition defineStep(String keyword, String description) {
		Matcher matcher = _pattern.matcher(description);
		if (!(matcher.matches())) {
			return null;
		}
		
		WorkflowCounterAssertionSD result = new WorkflowCounterAssertionSD();
		result.setScenarioState(_scenarioState);
		result._workflowName = matcher.group(1);
		result._counterKey = matcher.group(2);
		result._assertionDescription = matcher.group(3);
		
		Matcher assertionMatcher = EQUALS_PATTERN.matcher(result._assertionDescription);
		if (assertionMatcher.matches()) {
			long expectedValue = Long.parseLong(assertionMatcher.group());
			result._assertion = new EqualsAssertion(expectedValue);
			return result;
		}
		
		assertionMatcher = LESS_THAN_PATTERN.matcher(result._assertionDescription);
		if (assertionMatcher.matches()) {
			long minInvalidValue = Long.parseLong(assertionMatcher.group(1));
			result._assertion = new LessThanAssertion(minInvalidValue);
			return result;
		}
		
		assertionMatcher = AT_MOST_PATTERN.matcher(result._assertionDescription);
		if (assertionMatcher.matches()) {
			long maxValue = Long.parseLong(assertionMatcher.group(1));
			result._assertion = new AtMostAssertion(maxValue);
			return result;
		}
		
		assertionMatcher = AT_LEAST_PATTERN.matcher(result._assertionDescription);
		if (assertionMatcher.matches()) {
			long minValue = Long.parseLong(assertionMatcher.group(1));
			result._assertion = new AtLeastAssertion(minValue);
			return result;
		}
		
		assertionMatcher = GREATER_THAN_PATTERN.matcher(result._assertionDescription);
		if (assertionMatcher.matches()) {
			long maxInvalidValue = Long.parseLong(assertionMatcher.group(1));
			result._assertion = new GreaterThanAssertion(maxInvalidValue);
			return result;
		}
		
		String message =
			String.format(	"Unknown counter value assertion: '%s is %s'",
							result._counterKey,
							result._assertionDescription);
		throw new RuntimeException(message);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void run() throws StepFailureException {
		super.run();

		SimpleSSE<FlowResult> flowResultSSE = 
			(SimpleSSE<FlowResult>)(_scenarioState.get(_workflowName));
		if (flowResultSSE == null) {
			throw new StepFailureException("Workflow failed (no flow result)?");
		}
		FlowResult flowResult = flowResultSSE.getValue();
		Long counterValue = flowResult.getCounters().get(_counterKey);
		if (counterValue == null) {
			String assertionFailureString =
				String.format("%s was not present", _counterKey);
			throw new StepFailureException(		"Counter assertion failed: "
											+ 	assertionFailureString);
		}
		if (!(_assertion.isSatisfied(counterValue))) {
			String assertionFailureString =
				String.format(	"%s should be %s, but is %d",
								_counterKey,
								_assertionDescription,
								counterValue);
			throw new StepFailureException(		"Counter assertion failed: "
											+ 	assertionFailureString);
		}
	}

}
