package com.scaleunlimited.cascading.cuke;

import gherkin.parser.Parser;

import org.junit.Test;

import com.scaleunlimited.cascading.cuke.stepdefinitions.WordCountSD;
import com.scaleunlimited.cascading.cuke.stepdefinitions.WorkflowCounterAssertionSD;
import com.scaleunlimited.cascading.cuke.stepdefinitions.WorkflowParameterSD;


public class CascadingFormatterTest {
	
	@Test
	public void testWordCountFlow() throws Throwable {
		CascadingFormatter formatter =
			new CascadingFormatter(System.out, false, true);
		
		formatter.addStepDefinition(new WorkflowParameterSD());
		formatter.addStepDefinition(new WordCountSD());
		formatter.addStepDefinition(new WorkflowCounterAssertionSD());
		
		StringBuilder featureSource = new StringBuilder();
		featureSource.append("Feature: WordCountTool\n");
		featureSource.append("Scenario: Correctly computes word frequencies\n");
		featureSource.append("When the WordCountTool workflow is run\n");
		featureSource.append("And the inputText parameter is Now is the time for all good men to come to the aid of their country.\n");
		
		// These should all pass:
		featureSource.append("Then the WordCountTool com.scaleunlimited.cascading.cuke.stepdefinitions.WordCountSD$WordCountCounters.TOTAL_WORDS counter value is 16\n");
		featureSource.append("And the WordCountTool WordCountCounters.FREQUENCY_BY_WORD.the counter value is 2\n");
		featureSource.append("And the WordCountTool WordCountCounters.FREQUENCY_BY_WORD.to counter value is at least 2\n");
		featureSource.append("And the WordCountTool WordCountCounters.FREQUENCY_BY_WORD.good counter value is >=1\n");
		featureSource.append("And the WordCountTool WordCountCounters.FREQUENCY_BY_WORD.their counter value is at most 1\n");
		
		// This one should fail (as there's only one instance of "men"):
		featureSource.append("And the WordCountTool WordCountCounters.FREQUENCY_BY_WORD.men counter value is greater than 1\n");
		
		// This one should pass:
		featureSource.append("And the WordCountTool WordCountCounters.FREQUENCY_BY_WORD.come counter value is less than 2\n");
		
		// This one has no definition:
		featureSource.append("And this undefined assertion would still need to be implemented\n");
		
		// This one would pass, but it gets skipped because the previous one 
		// was undefined:
		featureSource.append("And the WordCountTool WordCountCounters.FREQUENCY_BY_WORD.men counter value is 1\n");

		Parser parser = new Parser(formatter);
		parser.parse(featureSource.toString(), "", 0);
        formatter.close();
        
        // TODO Figure out how to validate the output
	}
}
