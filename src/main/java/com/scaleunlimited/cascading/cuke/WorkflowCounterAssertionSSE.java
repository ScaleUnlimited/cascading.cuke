package com.scaleunlimited.cascading.cuke;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkflowCounterAssertionSSE implements ScenarioStateElement {
    private static final Logger LOGGER = LoggerFactory.getLogger(WorkflowCounterAssertionSSE.class);

    private static final String ASSERTION_KEY_TOKEN = "-assertion-";
	private static final int MAX_ASSERTIONS_PER_COUNTER = 100;
	
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
	
	String _counterKey;
	String _assertionDescription;
	Assertion _assertion;
	
	public WorkflowCounterAssertionSSE(	String counterKey,
										String assertionDescription) {
		super();
		_counterKey = counterKey;
		_assertionDescription = assertionDescription.trim();
		Matcher matcher = EQUALS_PATTERN.matcher(_assertionDescription);
		if (matcher.matches()) {
			long expectedValue = Long.parseLong(matcher.group());
			_assertion = new EqualsAssertion(expectedValue);
			return;
		}
		
		matcher = LESS_THAN_PATTERN.matcher(_assertionDescription);
		if (matcher.matches()) {
			long minInvalidValue = Long.parseLong(matcher.group(1));
			_assertion = new LessThanAssertion(minInvalidValue);
			return;
		}
		
		matcher = AT_MOST_PATTERN.matcher(_assertionDescription);
		if (matcher.matches()) {
			long maxValue = Long.parseLong(matcher.group(1));
			_assertion = new AtMostAssertion(maxValue);
			return;
		}
		
		matcher = AT_LEAST_PATTERN.matcher(_assertionDescription);
		if (matcher.matches()) {
			long minValue = Long.parseLong(matcher.group(1));
			_assertion = new AtLeastAssertion(minValue);
			return;
		}
		
		matcher = GREATER_THAN_PATTERN.matcher(_assertionDescription);
		if (matcher.matches()) {
			long maxInvalidValue = Long.parseLong(matcher.group(1));
			_assertion = new GreaterThanAssertion(maxInvalidValue);
			return;
		}
		
		String message =
			String.format(	"Unknown counter value assertion: '%s is %s'",
							_counterKey,
							_assertionDescription);
		throw new RuntimeException(message);
	}

	// TODO This should probably be throwing an Exception instead, but then
	// checkScenarioAssertions would need access to the Formatter in order
	// to call result(), or perhaps we let the first assertion failure mask
	// the results from all the following ones?  Ken is leaning that way.
	public boolean isSatisfied(Long counterValue) {
		if (counterValue == null) {
			String assertionFailureString =
				String.format("%s was not present", _counterKey);
			LOGGER.error("Counter assertion failed: " + assertionFailureString);
			return false;
		}
		boolean result = _assertion.isSatisfied(counterValue);
		if (!result) {
			String assertionFailureString =
				String.format(	"%s should be %s, but is %d",
								_counterKey,
								_assertionDescription,
								counterValue);
			LOGGER.error("Counter assertion failed: " + assertionFailureString);
		}
		return result;
	}

	@Override
	public void close() throws IOException {
		// nothing to release
	}
	
	public void addToScenario(ScenarioState scenarioState) {
		for (int i = 0; i < MAX_ASSERTIONS_PER_COUNTER; i++) {
			String assertionKey = makeAssertionKey(i);
			if (!(scenarioState.containsKey(assertionKey))) {
				scenarioState.put(assertionKey, this);
				return;
			}
		}
		String message = 
			String.format(	"More than %d assertions for counter %s!",
							MAX_ASSERTIONS_PER_COUNTER,
							_counterKey);
		throw new RuntimeException(message);
	}

	public static boolean checkScenarioAssertions(	Map<String, Long> counters, 
													ScenarioState scenarioState) {
		boolean result = true;
		for (Map.Entry<String, ScenarioStateElement> stateEntry : scenarioState.entrySet()) {
			ScenarioStateElement stateElement = stateEntry.getValue();
			if (stateElement instanceof WorkflowCounterAssertionSSE) {
				WorkflowCounterAssertionSSE assertion =
					(WorkflowCounterAssertionSSE)stateElement;
				if (!(assertion.isSatisfied(counters.get(assertion._counterKey)))) {
					result = false;
				}
			}
		}
		return result;
	}
	
	private String makeAssertionKey(int i) {
		return (_counterKey + ASSERTION_KEY_TOKEN + i);
	}

}
