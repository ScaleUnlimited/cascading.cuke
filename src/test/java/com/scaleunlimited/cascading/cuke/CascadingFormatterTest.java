package com.scaleunlimited.cascading.cuke;

import static org.junit.Assert.assertEquals;
import gherkin.parser.Parser;

import java.util.regex.Matcher;

import org.junit.Test;


public class CascadingFormatterTest {
    public static class WorkflowParameterSD extends BaseStepDefinition {
		public WorkflowParameterSD() {
			super();
			setRegex("the (.+) parameter is (.+)");
		}

		@Override
		public boolean isMatchesStep(String keyword, String description) {
			Matcher matcher = _pattern.matcher(description);
			boolean result = matcher.matches();
			if (result) {
				String parameterName = matcher.group(1);
				StringSSE parameterValue = new StringSSE(matcher.group(2));
				_scenarioState.put(parameterName, parameterValue);
			}
			return result;
		}
    }
	
    public static class WorkflowSD extends BaseStepDefinition {
		public WorkflowSD() {
			super();
			setRegex("the MerchantScoreTool workflow is run");
		}

		@Override
		public void run() {
			super.run();
			assertEquals(	"2014-08-02", 
							_scenarioState.get("targetDate").toString());
			assertEquals(	30,
							Integer.parseInt(_scenarioState.get("backtrace").toString()));
			assertEquals(	"working", 
							_scenarioState.get("workingDir").toString());
			_scenarioState.put("MerchantScoreToolResult", new StringSSE("success"));
		}
    }
    
    public static class WorkflowResultSD extends BaseStepDefinition {
    	public WorkflowResultSD() {
			super();
			setRegex("the MerchantScoreTool workflow got the expected parameters.");
    	}

		@Override
		public void run() {
			super.run();
			assertEquals(	"success", 
							_scenarioState.get("MerchantScoreToolResult").toString());
		}
    }
	
	@Test
	public void testPassingParametersViaState() throws Throwable {
		CascadingFormatter formatter =
			new CascadingFormatter(System.out, false, true);
		
		formatter.addStepDefinition(new WorkflowParameterSD());
		formatter.addStepDefinition(new WorkflowSD());
		formatter.addStepDefinition(new WorkflowResultSD());
		
		StringBuilder featureSource = new StringBuilder();
		featureSource.append("Feature: MerchantScoreTool\n");
		featureSource.append("Scenario: Accesses parameters\n");
		featureSource.append("When the MerchantScoreTool workflow is run\n");
		featureSource.append("And the targetDate parameter is 2014-08-02\n");
		featureSource.append("And the backtrace parameter is 30\n");
		featureSource.append("And the workingDir parameter is working\n");
		featureSource.append("Then the MerchantScoreTool workflow got the expected parameters.\n");
        new Parser(formatter).parse(featureSource.toString(), "", 0);
        formatter.close();
	}
}
