package com.scaleunlimited.cascading.cuke;

import java.util.HashMap;
import java.util.Map;

import cascading.tuple.TupleEntryCollector;
import cucumber.api.java.en.Given;

/**
 * Additional step definitions used by the letter count tool
 *
 */
public class LetterCountStepDefs {

    @Given("^(\\d+) random \"([^\"]*)\" records in the workflow \"([^\"]*)\" directory$")
    public void xxx_random_yyy_records_in_the_workflow_zzz_directory(int numRecords, String recordName, String directoryName) throws Throwable {
        WorkflowContext context = WorkflowContext.getCurrentContext();
        WorkflowInterface workflow = context.getWorkflow();
        TupleEntryCollector writer = workflow.openBinaryForWrite(context, directoryName, recordName);
        
        Map<String, String> tupleValues = new HashMap<>();
        
        for (int i = 0; i < numRecords; i++) {
            writer.add(workflow.createTuple(context, recordName, new TupleValues(tupleValues)));
        }

        writer.close();
    }

}
