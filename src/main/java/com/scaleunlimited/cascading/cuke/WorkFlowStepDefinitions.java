package com.scaleunlimited.cascading.cuke;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.local.LocalFlowProcess;
import cascading.flow.planner.PlannerException;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.local.FileTap;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

import com.scaleunlimited.cascading.FlowResult;
import com.scaleunlimited.cascading.FlowRunner;

import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;

@SuppressWarnings("unchecked")
public class WorkFlowStepDefinitions {

    @Given("^the (.+) package contains the (.+) workflow$")
    public void the_package_contains_the_workflow(String packageName, String workflowName) throws Throwable {
        Class clazz = Class.forName(packageName + "." + workflowName);
        WorkflowContext.registerWorkflow(workflowName, clazz);
    }

    @Given("^the workflow will be run (locally| on a cluster)$")
    public void the_workflow_will_be_run_yyy(String platformName) throws Throwable {
    	String workflowName = WorkflowContext.getCurrentWorkflowName();
        WorkflowContext.getContext(workflowName).setDefaultPlatform(getPlatform(workflowName, platformName));

    }

    @Given("^these parameters for the workflow:$")
    public void these_parameters_for_the_workflow(List<List<String>> parameters) throws Throwable {
        WorkflowContext context = WorkflowContext.getCurrentContext();

        // We get one entry (list) for each row, with the first element being the
        // parameter name, and the second element being the parameter value.
        for (List<String> parameter : parameters) {
            context.addParameter(parameter.get(0), parameter.get(1));
        }
    }

    @Given("^the workflow parameter (.+) is (.+)$")
    public void the_workflow_parameter_xxx_is_yyy(String paramName, String paramValue) throws Throwable {
        WorkflowContext context = WorkflowContext.getCurrentContext();
        context.addParameter(paramName, paramValue);
    }

    @SuppressWarnings("rawtypes")
    @When("^the workflow is run$")
    public void the_workflow_is_run() throws Throwable {
        WorkflowContext context = WorkflowContext.getCurrentContext();
        WorkflowPlatform platform = WorkflowContext.getCurrentPlatform();
        
        try {
            WorkflowInterface workflow = context.getWorkflow();
            
            Flow flow = workflow.createFlow(platform, context.getParamsCopy());
            FlowResult result = FlowRunner.run(flow);
            context.addResult(result);
        } catch (PlannerException e) {
            throw new AssertionError(String.format("Workflow run step failed: the plan for workflow %s is invalid: %s", WorkflowContext.getCurrentWorkflowName(), e.getMessage()));
        } catch (ClassNotFoundException e) {
            throw new AssertionError(String.format("Workflow run step failed: the workflow class %s doesn't exist", WorkflowContext.getCurrentWorkflowName()));
        } catch (Exception e) {
            context.addFailure(e);
        }
    }
    
    @Then("^the workflow should fail$")
    public void the_workflow_should_fail() throws Throwable {
        WorkflowContext context = WorkflowContext.getCurrentContext();

        if (context.getFailure() == null) {
            throw new AssertionError(String.format("The workflow %s ran without an error", WorkflowContext.getCurrentWorkflowName()));
        }
    }

    @Given("^the workflow \"(.*?)\" directory has been deleted$")
    public void the_workflow_xxx_directory_has_been_deleted(String directoryName) throws Throwable {
        WorkflowPlatform platform = WorkflowContext.getCurrentPlatform();
        if (platform == WorkflowPlatform.DISTRIBUTED) {
            Tap tap = new Hfs(new cascading.scheme.hadoop.TextLine(), directoryName, SinkMode.REPLACE);
            tap.deleteResource(new HadoopFlowProcess());
        } else if (platform == WorkflowPlatform.LOCAL) {
        	Tap tap = new FileTap(new cascading.scheme.local.TextLine(), directoryName, SinkMode.REPLACE);
        	tap.deleteResource(new LocalFlowProcess());
        }
    }

    @Given("^text records in the workflow \"(.*?)\" directory:$")
    public void text_records_in_the_workflow_directory(String directoryName, List<String> textLines) throws Throwable {
        TupleEntryCollector writer = null;
        
        WorkflowPlatform platform = WorkflowContext.getCurrentPlatform();
        if (platform == WorkflowPlatform.DISTRIBUTED) {
            Tap tap = new Hfs(new cascading.scheme.hadoop.TextLine(), directoryName, SinkMode.REPLACE);
            writer = tap.openForWrite(new HadoopFlowProcess());
        } else if (platform == WorkflowPlatform.LOCAL) {
            Tap tap = new FileTap(new cascading.scheme.local.TextLine(), directoryName, SinkMode.REPLACE);
            writer = tap.openForWrite(new LocalFlowProcess());
        }
        
        for (String line : textLines) {
            writer.add(new Tuple(line));
        }
        
        writer.close();
    }
    
    @Then("^the workflow \"(.*?)\" (?:result|results|output|file) should have a record where:$")
    public void the_workflow__xxx_results_should_have_a_record_where(String resultsName, List<List<String>> targetValues) throws Throwable {
        WorkflowInterface workflow = WorkflowContext.getCurrentContext().getWorkflow();
        TupleEntryIterator iter = workflow.openBinaryForRead(WorkflowContext.getCurrentPlatform(), WorkflowContext.getCurrentContext().getParamsCopy(), resultsName);
        while (iter.hasNext()) {
            TupleEntry te = iter.next();
            if (tupleMatchesTarget(te, targetValues)) {
                return;
            }
        }
        
        throw new AssertionError(String.format("No record found for workflow %s in results \"%s\" that matched the target value", 
        		WorkflowContext.getCurrentWorkflowName(), resultsName));
    }
    
    @Then("^the workflow \"(.*?)\" (?:result|results|output|file) should have records where:$")
    public void the_workflow_results_in_the_xxx_directory_should_have_records_where(String resultsName, List<Map<String, String>> targetValues) throws Throwable {
    	matchResults(resultsName, targetValues, true);
    }

	@Then("^the workflow \"(.*?)\" (?:result|results|output|file) should only have records where:$")
    public void the_workflow_result_should_only_have_records_where(String resultsName, List<Map<String, String>> targetValues) throws Throwable {
    	matchResults(resultsName, targetValues, false);
    }
    
	@Then("^the workflow \"(.*?)\" (?:result|results|output|file) should not have records where:$")
	public void the_workflow_xxx_result_should_not_have_records_where(String resultsName, List<Map<String, String>> excludedValues) throws Throwable {
	    dontMatchResults(resultsName, excludedValues);
	}

	@Then("^the workflow \"(.*?)\" (?:counter|counter value) should be (\\d+)$")
	public void the_workflow_counter_should_be(String counterName, long value) throws Throwable {
	    checkCounter(counterName, value, value);
	}

	@Then("^the workflow \"(.*?)\" (?:counter|counter value) should be (?:more than|greater than|>) (\\d+)$")
	public void the_workflow_counter_should_be_more_than(String counterName, long value) throws Throwable {
	    checkCounter(counterName, value + 1, Long.MAX_VALUE);
	}

	@Then("^the workflow \"(.*?)\" (?:counter|counter value) should be (?:at least|>=) (\\d+)$")
	public void the_workflow_counter_should_be_at_least(String counterName, long value) throws Throwable {
	    checkCounter(counterName, value, Long.MAX_VALUE);
	}

	@Then("^the workflow \"(.*?)\" (?:counter|counter value) should be (?:less than|<) (\\d+)$")
	public void the_workflow_counter_should_be_less_than(String counterName, long value) throws Throwable {
	    checkCounter(counterName, Long.MIN_VALUE, value + 1);
	}

	@Then("^the workflow \"(.*?)\" (?:counter|counter value) should be (?:at most|<=) (\\d+)$")
	public void the_workflow_counter_should_be_at_most(String counterName, long value) throws Throwable {
	    checkCounter(counterName, Long.MIN_VALUE, value);
	}

    private void checkCounter(String targetCounterName, long minValue, long maxValue) {
    	Long counterValue = null;
    	String matchedCounterName = null;
    	Map<String, Long> counters = WorkflowContext.getCurrentContext().getCounters();
    	for (String counterName : counters.keySet()) {
    		if (counterName.equals(targetCounterName) || counterName.endsWith("." + targetCounterName)) {
    			if (matchedCounterName != null) {
    	            throw new AssertionError(String.format("Counter \"%s\" for workflow %s has multiple matches: \"%s\" and \"%s\"",
    	            		targetCounterName,
    	            		WorkflowContext.getCurrentWorkflowName(),
    	            		matchedCounterName,
    	            		counterName));

    			}
    			
    			matchedCounterName = counterName;
    			counterValue = counters.get(counterName);
    		}
    	}
    	
    	// If we can't find the counter, then it has an implicit value of 0
    	if (counterValue == null) {
    		counterValue = 0L;
    	}
    	
    	if (counterValue < minValue) {
    		throw new AssertionError(String.format("Counter \"%s\" for workflow %s is too small, was %d, must be at least %d",
            		targetCounterName,
            		WorkflowContext.getCurrentWorkflowName(),
            		counterValue, 
            		minValue));
    	} else if (counterValue > maxValue) {
    		throw new AssertionError(String.format("Counter \"%s\" for workflow %s is too bog, was %d, must be no more than %d",
            		targetCounterName,
            		WorkflowContext.getCurrentWorkflowName(),
            		counterValue, 
            		maxValue));
    	}
	}

	private void matchResults(String resultsName, List<Map<String, String>> targetValues, boolean allowUnmatchedResults) throws Throwable  {
        // For every TupleEntry, see if we have a match with one of our target records.
        // If so, remove its index from the list, and make sure the list is empty when we're done.
        List<Map<String, String>> remainingValues = new ArrayList<Map<String,String>>(targetValues);
        
        WorkflowContext context = WorkflowContext.getCurrentContext();
        WorkflowInterface workflow = context.getWorkflow();
        WorkflowPlatform platform = WorkflowContext.getCurrentPlatform();
        TupleEntryIterator iter = workflow.openBinaryForRead(platform, WorkflowContext.getCurrentContext().getParamsCopy(), resultsName);
        while (iter.hasNext()) {
            TupleEntry te = iter.next();
            
            boolean foundMatch = false;
            for (int i = 0; i < remainingValues.size() && !foundMatch; i++) {
                Map<String, String> remainingValue = remainingValues.get(i);
                if (tupleMatchesTarget(te, remainingValue)) {
                    foundMatch = true;
                    remainingValues.remove(i);
                }
            }
            
            if (!foundMatch && !allowUnmatchedResults) {
                throw new AssertionError(String.format("Record \"%s\" found for workflow %s in results \"%s\" that weren't in the target list",
                		te,
                        context.getWorkflowName(), 
                        resultsName));
            }
        }
        
        if (!remainingValues.isEmpty()) {
            throw new AssertionError(String.format("No record found for workflow %s in results \"%s\" that matched the target value %s",
                    context.getWorkflowName(), 
                    resultsName,
                    remainingValues.get(0)));
        }
	}

    private void dontMatchResults(String resultsName, List<Map<String, String>> excludedValues) throws Throwable  {
        WorkflowContext context = WorkflowContext.getCurrentContext();
        WorkflowInterface workflow = context.getWorkflow();
        WorkflowPlatform platform = WorkflowContext.getCurrentPlatform();
        TupleEntryIterator iter = workflow.openBinaryForRead(platform, WorkflowContext.getCurrentContext().getParamsCopy(), resultsName);
        while (iter.hasNext()) {
            TupleEntry te = iter.next();
            
            for (int i = 0; i < excludedValues.size(); i++) {
                Map<String, String> excludedValue = excludedValues.get(i);
                if (tupleMatchesTarget(te, excludedValue)) {
                    throw new AssertionError(String.format("Record \"%s\" found for workflow %s in results \"%s\" that was in the excluded list",
                    		te,
                            context.getWorkflowName(), 
                            resultsName));
                }
            }
        }
	}

    private boolean tupleMatchesTarget(TupleEntry te, Map<String, String> targetValues) {
        for (String fieldName : targetValues.keySet()) {
            String tupleValue = te.getString(fieldName);
            String targetValue = targetValues.get(fieldName);
            if ((tupleValue == null) && !targetValue.equals("null")) {
                return false;
            } else if ((tupleValue != null) && !tupleValue.equals(targetValue)) {
                return false;
            }
        }
        
        return true;
    }
    
    private boolean tupleMatchesTarget(TupleEntry te, List<List<String>> targetValues) {
        for (List<String> fieldAndValue : targetValues) {
            String tupleValue = te.getString(fieldAndValue.get(0));
            if ((tupleValue != null) && tupleValue.equals(fieldAndValue.get(1))) {
                return true;
            }
        }
        
        return false;
    }
    
    private WorkflowPlatform getPlatform(String workflowName, String platformName) {
        platformName = platformName.trim();
        
        if (platformName.isEmpty()) {
            return WorkflowContext.getContext(workflowName).getDefaultPlatform();
        } else if (platformName.equals("locally") || platformName.equals("local")) {
            return WorkflowPlatform.LOCAL;
        } else if (platformName.equals("on a cluster") || platformName.equals("hdfs")) {
            return WorkflowPlatform.DISTRIBUTED;
        } else {
            throw new IllegalArgumentException(String.format("The workflow platform \"%s\" is unknown", platformName));
        }
    }
}
