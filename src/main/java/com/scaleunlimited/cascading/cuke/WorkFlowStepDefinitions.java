package com.scaleunlimited.cascading.cuke;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.PathNotFoundException;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.planner.PlannerException;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cucumber.api.DataTable;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;

import com.scaleunlimited.cascading.FlowResult;
import com.scaleunlimited.cascading.FlowRunner;

@SuppressWarnings("unchecked")
public class WorkFlowStepDefinitions {

    @Given("^the (.+) package contains the (.+) workflow$")
    public void the_package_contains_the_workflow(String packageName, String workflowName) throws Throwable {
        Class clazz = Class.forName(packageName + "." + workflowName);
        WorkflowContext.registerWorkflow(workflowName, clazz);
    }

    @Given("^the workflow will be run (locally| on a cluster) with test directory \"(.+)\"$")
    public void the_workflow_will_be_run_xxx_with_test_directory_yyy(String platformName, String testDir) throws Throwable {
    	String workflowName = WorkflowContext.getCurrentWorkflowName();
        WorkflowContext.getContext(workflowName).setDefaultPlatform(getPlatform(workflowName, platformName));

    	// Fix up common issue with not having trailing '/'
    	if (!testDir.endsWith("/")) {
    		testDir = testDir + "/";
    	}

    	WorkflowContext.getCurrentContext().setTestDir(testDir);

    	// TODO figure out if we need to support a deferred deletion of the directory, so that callers can
    	// tell us that we shouldn't clear out the test directory.
    	the_workflow_test_directory_is_empty();
    }

    @SuppressWarnings("rawtypes")
	@Given("^the workflow test directory is empty")
    public void the_workflow_test_directory_is_empty() throws Throwable {
        WorkflowPlatform platform = WorkflowContext.getCurrentPlatform();
        if (platform == WorkflowPlatform.DISTRIBUTED) {
            Tap tap = new Hfs(new cascading.scheme.hadoop.TextLine(), WorkflowContext.getCurrentContext().getTestDir(), SinkMode.REPLACE);
            tap.deleteResource(new HadoopFlowProcess());
        } else if (platform == WorkflowPlatform.LOCAL) {
        	// We can't use a FileTap to get rid of this directory, as that only tries to delete a file if you call tap.deleteResource
        	File f = new File(WorkflowContext.getCurrentContext().getTestDir());
        	if (f.isDirectory()) {
        		FileUtils.deleteDirectory(f);
        	} else {
        		f.delete();
        	}
        } else {
    		throw new RuntimeException("Unknown Workflow platform " + platform);
        }
    }

	@Given("^these parameters for the workflow:$")
    public void these_parameters_for_the_workflow(List<List<String>> parameters) throws Throwable {
        WorkflowContext context = WorkflowContext.getCurrentContext();

        // We get one entry (list) for each row, with the first element being the
        // parameter name, and the second element being the parameter value.
        // We want to support expansion of ${testdir} for paths.
        for (List<String> parameter : parameters) {
        	if (parameter.size() == 2) {
        		context.addParameter(parameter.get(0), expandMacros(context, parameter.get(1)));
        	} else {
        		throw new IllegalArgumentException("Workflow parameters must be two column format (| name | value |)");
        	}
        }
    }

	protected String expandMacros(WorkflowContext context, String s) {
		return s.replaceAll("\\$\\{testdir\\}", context.getTestDir());
	}

    @Given("^the workflow parameter \"(.+?)\" is \"(.+)\"$")
    public void the_workflow_parameter_xxx_is_yyy(String paramName, String paramValue) throws Throwable {
        WorkflowContext context = WorkflowContext.getCurrentContext();
        context.addParameter(paramName, expandMacros(context, paramValue));
    }

    @SuppressWarnings("rawtypes")
    @When("^the workflow run is attempted$")
    public void the_workflow_run_is_attempted() throws Throwable {
        WorkflowContext context = WorkflowContext.getCurrentContext();

        try {
            WorkflowInterface workflow = context.getWorkflow();
            Flow flow = workflow.createFlow(context);
            FlowResult result = FlowRunner.run(flow);
            context.addResult(result);
        } catch (PlannerException e) {
            throw new AssertionError(String.format("Workflow run step failed: the plan for workflow %s is invalid: %s", context.getWorkflowName(), e.getMessage()));
        } catch (ClassNotFoundException e) {
            throw new AssertionError(String.format("Workflow run step failed: the workflow class %s doesn't exist", context.getWorkflowName()));
        } catch (Exception e) {
            context.addFailure(e);
        }
    }


    @SuppressWarnings("rawtypes")
    @When("^the workflow is run$")
    public void the_workflow_is_run() throws Throwable {
    	WorkflowContext context = WorkflowContext.getCurrentContext();

        WorkflowInterface workflow = context.getWorkflow();
    	Flow flow = workflow.createFlow(context);
    	FlowResult result = FlowRunner.run(flow);
    	context.addResult(result);
    }

    @SuppressWarnings("rawtypes")
    @When("^the workflow is run with these additional parameters:$")
    public void the_workflow_is_run_with_these_additional_parameters(List<List<String>> parameters) throws Throwable {
        WorkflowContext context = WorkflowContext.getCurrentContext();
        WorkflowParams before = context.getParams();

        // We get one entry (list) for each row, with the first element being the
        // parameter name, and the second element being the parameter value.
        // We want to support expansion of ${testdir} for paths.
        for (List<String> parameter : parameters) {
            if (parameter.size() == 2) {
                context.addParameter(parameter.get(0), expandMacros(context, parameter.get(1)));
            } else {
                throw new IllegalArgumentException("Workflow parameters must be two column format (| name | value |)");
            }
        }

        WorkflowInterface workflow = context.getWorkflow();
        Flow flow = workflow.createFlow(context);
        FlowResult result = FlowRunner.run(flow);
        context.addResult(result);

        context.resetParameters(before);
    }

    @Then("^the workflow should fail$")
    public void the_workflow_should_fail() throws Throwable {
        WorkflowContext context = WorkflowContext.getCurrentContext();

        if (context.getFailure() == null) {
            throw new AssertionError(String.format("The workflow %s ran without an error", WorkflowContext.getCurrentWorkflowName()));
        }
    }

    @Given("^these text records in the workflow \"(.*?)\" directory:$")
    public void these_text_records_in_the_workflow_xxx_directory(String directoryName, List<String> textLines) throws Throwable {
        WorkflowContext context = WorkflowContext.getCurrentContext();
        TupleEntryCollector writer = context.getWorkflow().openTextForWrite(context, directoryName);

        try {
        	for (String line : textLines) {
        		writer.add(new Tuple(line));
        	}
        } finally {
        	writer.close();
        }
    }

    @Given("^these \"(.+?)\" records in the workflow \"(.*?)\" directory:$")
    public void these_xxx_records_in_the_workflow_yyy_directory(String recordName, String directoryName, List<Map<String, String>> sourceValues) throws Throwable {
    	WorkflowContext context = WorkflowContext.getCurrentContext();
        WorkflowInterface workflow = context.getWorkflow();
        TupleEntryCollector writer = workflow.openBinaryForWrite(context, directoryName, recordName);

        // We need to create records that we write out.
        for (Map<String, String> tupleValues : sourceValues) {
        	writer.add(workflow.createTuple(context, recordName, new HashMap<String, String>(tupleValues)));
        }

        writer.close();
    }

    @Given("^(\\d+) \"(.*?)\" records in the workflow \"(.*?)\" directory:$")
    public void xxx_records_in_the_workflow_xxx_directory(int numRecords, String recordName, String directoryName, List<Map<String, String>> sourceValues) throws Throwable {
    	WorkflowContext context = WorkflowContext.getCurrentContext();
        WorkflowInterface workflow = context.getWorkflow();
        TupleEntryCollector writer = workflow.openBinaryForWrite(context, directoryName, recordName);
        // TODO set the caller set the random seed
        Random rand = new Random(1L);

        for (int i = 0; i < numRecords; i++) {
        	// Pick a random value to use.
        	// We have to clone, so that the createTuple() method can remove values to check for unsupported
        	// TODO make this more efficient? Do single check once to see if fields match up
        	Map<String, String> tupleValues = sourceValues.get(rand.nextInt(sourceValues.size()));
        	writer.add(workflow.createTuple(context, recordName, new HashMap<String, String>(tupleValues)));
        }

        writer.close();
    }

    @Then("^the workflow \"(.*?)\" (?:result|results|output|file|directory) should have a record where:$")
    public void the_workflow__xxx_results_should_have_a_record_where(String directoryName, DataTable targetValues) throws Throwable {
    	WorkflowContext context = WorkflowContext.getCurrentContext();
        WorkflowInterface workflow = context.getWorkflow();
        TupleEntryIterator iter = workflow.openBinaryForRead(context, directoryName);

        while (iter.hasNext()) {
            TupleEntry te = iter.next();
            if (tupleMatchesTarget(workflow, te, targetValues.asMap(String.class, String.class))) {
                return;
            }
        }

        throw new AssertionError(String.format("No record found for workflow %s in results \"%s\" that matched the target value",
        		WorkflowContext.getCurrentWorkflowName(), directoryName));
    }

    @Then("^the workflow \"(.*?)\" (?:result|results|output|file|directory) should have no records$")
    public void the_workflow__xxx_results_should_have_no_records(String directoryName) throws Throwable {
    	WorkflowContext context = WorkflowContext.getCurrentContext();
        WorkflowInterface workflow = context.getWorkflow();
        TupleEntryIterator iter = null;
        try {
            iter = workflow.openBinaryForRead(context, directoryName);
        } catch (PathNotFoundException e) {
        }
        int records = 0;
        if (iter != null) {
            while (iter.hasNext()) {
                iter.next();
                records++;
            }
        }
        if (records > 0)
            throw new AssertionError(String.format(records + " record(s) found for workflow %s in results \"%s\". Expected none.",
                WorkflowContext.getCurrentWorkflowName(), directoryName));
    }

    @Then("^the workflow \"(.*?)\" (?:result|results|output|file) should have records where:$")
    public void the_workflow_results_should_have_records_where(String directoryName, DataTable targetValues) throws Throwable {
    	matchResults(directoryName, targetValues, true);
    }

	@Then("^the workflow \"(.*?)\" (?:result|results|output|file) should only have records where:$")
    public void the_workflow_result_should_only_have_records_where(String directoryName, DataTable targetValues) throws Throwable {
    	matchResults(directoryName, targetValues, false);
    }

	@Then("^the workflow \"(.*?)\" (?:result|results|output|file) should not have records where:$")
	public void the_workflow_xxx_result_should_not_have_records_where(String directoryName, DataTable excludedValues) throws Throwable {
	    dontMatchResults(directoryName, excludedValues);
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

	@Then("^the workflow result should have counters where:$")
	public void the_workflow_result_should_have_counters_where(List<List<String>> targetValues) throws Throwable {
		for (List<String> counterAndValue : targetValues) {
			long targetCount = Long.parseLong(counterAndValue.get(1));
			checkCounter(counterAndValue.get(0), targetCount, targetCount);
		}
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

    protected void matchResults(String directoryName, DataTable targetValues, boolean allowUnmatchedResults) throws Throwable  {
        // For every TupleEntry, see if we have a match with one of our target records.
        // If so, remove its index from the list, and make sure the list is empty when we're done.
        List<Map<String, String>> remainingValues = new ArrayList<Map<String,String>>(targetValues.asMaps(String.class, String.class));

        WorkflowContext context = WorkflowContext.getCurrentContext();
        WorkflowInterface workflow = context.getWorkflow();
        TupleEntryIterator iter = workflow.openBinaryForRead(context, directoryName);
        while (iter.hasNext()) {
            TupleEntry te = iter.next();

            boolean foundMatch = false;
            int leastDiffs = Integer.MAX_VALUE;
            Set<TupleDiff> leastDiffsSet = null;

            for (int i = 0; i < remainingValues.size() && !foundMatch; i++) {
                Map<String, String> row = remainingValues.get(i);
                Set<TupleDiff> tupleDiffs = diffTupleAndTarget(workflow, te, row);
                if(tupleDiffs.size() < leastDiffs) {
                    leastDiffs = tupleDiffs.size();
                    leastDiffsSet = tupleDiffs;
                }
                if (tupleDiffs.size() == 0) {
                    foundMatch = true;
                    remainingValues.remove(i);
                }
            }

            if (!foundMatch && !allowUnmatchedResults) {
                StringBuilder sb = new StringBuilder();
                sb.append(String.format("Record \"%s\" found for workflow %s in results \"%s\" that weren't in the target list",
                    te,
                    context.getWorkflowName(),
                    directoryName));
                sb.append("\n");
                for (TupleDiff diff: leastDiffsSet) {
                    sb.append(diff.toString()).append("\n");
                }
                throw new AssertionError(sb.toString());
            }
        }

        if (!remainingValues.isEmpty()) {
            throw new AssertionError(String.format("No record found for workflow %s in results \"%s\" that matched the target value %s",
                    context.getWorkflowName(),
                    directoryName,
                    remainingValues.get(0)));
        }
	}

    protected void dontMatchResults(String directoryName, DataTable excludedValues) throws Throwable  {
        WorkflowContext context = WorkflowContext.getCurrentContext();
        WorkflowInterface workflow = context.getWorkflow();
        TupleEntryIterator iter = workflow.openBinaryForRead(context, directoryName);
        List<Map<String, String>> excludedValuesMap = excludedValues.asMaps(String.class, String.class);
        while (iter.hasNext()) {
            TupleEntry te = iter.next();

            for (Map<String, String> row: excludedValuesMap) {
                if (tupleMatchesTarget(workflow, te, row)) {
                    throw new AssertionError(String.format("Record \"%s\" found for workflow %s in results \"%s\" that was in the excluded list",
                    		te,
                            context.getWorkflowName(),
                            directoryName));
                }
            }
        }
	}

    protected boolean tupleMatchesTarget(WorkflowInterface workflow, TupleEntry te, Map<String, String> targetValues) {
        return diffTupleAndTarget(workflow, te, targetValues).size() == 0;
    }

    protected Set<TupleDiff> diffTupleAndTarget(WorkflowInterface workflow, TupleEntry te, Map<String, String> targetValues) {
        return diffTupleAndTarget(workflow, te, targetValues, false);
    }

        /**
         * @param workflow     workflow
         * @param te           source
         * @param targetValues target
         * @param reportAdditionalColumns should TupleDiff.ADDITIONAL be reported?
         * @return
         */
    protected Set<TupleDiff> diffTupleAndTarget(WorkflowInterface workflow, TupleEntry te, Map<String, String> targetValues, boolean reportAdditionalColumns) {
        Set<TupleDiff> diffs = new LinkedHashSet<TupleDiff>();
        for (Map.Entry<String, String> entry : targetValues.entrySet()) {
            String fieldName = entry.getKey();
            final TupleDiff diff = workflow.diffTupleAndTarget(te, fieldName, entry.getValue());
            if(diff != null) {
                diffs.add(diff);
            }
        }

        if(reportAdditionalColumns) {
            for (Iterator it = te.getFields().iterator(); it.hasNext(); ) {
                String field = (String) it.next();
                final String actual = te.getString(field);
                if (actual != null && !targetValues.containsKey(field)) {
                    diffs.add(new TupleDiff(field, null, actual, TupleDiff.Type.ADDITIONAL));
                }
            }
        }

        return diffs;
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
