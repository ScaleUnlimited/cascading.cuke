package com.scaleunlimited.cascading.cuke;

import java.util.Map;

import com.scaleunlimited.cascading.FlowResult;

import cascading.flow.Flow;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

public abstract class BaseWorkflowInterface implements WorkflowInterface {
    
    @Override 
    public Flow createFlow(WorkflowContext context) throws Throwable {
        throw new UnsupportedOperationException();
    }

    @Override
    public TupleDiff diffTupleAndTarget(TupleEntry te, String field, String value) {
        return WorkflowUtils.diffTupleAndTarget(te, field, value);
    }

    @Override
    public TupleEntryIterator openBinaryForRead(WorkflowContext context, String path) throws Throwable {
        throw new UnsupportedOperationException();
    }

    @Override
    public TupleEntryCollector openBinaryForWrite(WorkflowContext context, String path, String recordName) throws Throwable {
        throw new UnsupportedOperationException();
    }

    @Override
    public TupleEntryCollector openTextForWrite(WorkflowContext context, String path) throws Throwable {
        throw new UnsupportedOperationException();
    }

    @Override
    public TupleEntryIterator openTextForRead(WorkflowContext context, String path) throws Throwable {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isBinary(String path) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public Tuple createTuple(WorkflowContext context, String recordName, TupleValues tupleValues) throws Throwable {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public FlowResult runTool(WorkflowContext context) throws Throwable {
        throw new UnsupportedOperationException();
    }
}
