package com.scaleunlimited.cascading.cuke;

import java.util.Map;

import cascading.flow.Flow;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

public interface WorkflowInterface {

    @SuppressWarnings("rawtypes")
    public Flow createFlow(WorkflowContext context) throws Throwable;
    
    public Tuple createTuple(WorkflowContext context, String recordName, Map<String, String> tupleValues) throws Throwable;

    public TupleEntryIterator openBinaryForRead(WorkflowContext context, String path) throws Throwable;
    public TupleEntryCollector openBinaryForWrite(WorkflowContext context, String path, String recordName) throws Throwable;

    public TupleEntryCollector openTextForWrite(WorkflowContext context, String path) throws Throwable;

    public TupleEntryIterator openTextForRead(WorkflowContext context, String path) throws Throwable;

    public boolean isBinary(String path);

    // returns null if tupleField matches target value, or the type of TupleDiff.
    public TupleDiff diffTupleAndTarget(TupleEntry te, String field, String value);
}
