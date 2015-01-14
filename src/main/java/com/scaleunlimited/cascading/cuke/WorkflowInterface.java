package com.scaleunlimited.cascading.cuke;

import java.util.Map;

import com.scaleunlimited.cascading.FlowResult;

import cascading.flow.Flow;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

public interface WorkflowInterface {

    @SuppressWarnings("rawtypes")
    public Flow createFlow(WorkflowContext context) throws Throwable;
    
    public Tuple createTuple(WorkflowContext context, String recordName, TupleValues tupleValues) throws Throwable;

    public TupleEntryIterator openBinaryForRead(WorkflowContext context, String path) throws Throwable;
    public TupleEntryCollector openBinaryForWrite(WorkflowContext context, String path, String recordName) throws Throwable;

    public TupleEntryCollector openTextForWrite(WorkflowContext context, String path) throws Throwable;

    public TupleEntryIterator openTextForRead(WorkflowContext context, String path) throws Throwable;

    public boolean isBinary(String path);

    // returns null if tupleField matches target value, or the type of TupleDiff.
    public TupleDiff diffTupleAndTarget(TupleEntry te, String field, String value);
    
    // Some workflows need to be run as a top-level tool, not a flow. But we still want to return
    // the results of any Flow that might be run by the tool.
    public FlowResult runTool(WorkflowContext context) throws Throwable;
}
