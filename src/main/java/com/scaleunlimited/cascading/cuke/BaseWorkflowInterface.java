package com.scaleunlimited.cascading.cuke;

import java.util.Map;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

public abstract class BaseWorkflowInterface implements WorkflowInterface {
    @Override public TupleMatchFailure tupleFieldMatchesTarget(TupleEntry te, String field, String value) {
        return WorkflowUtils.tupleFieldMatchesTarget(te, field, value);
    }

    @Override public Tuple createTuple(WorkflowContext context, String recordName, Map<String, String> tupleValues) throws Throwable {
        throw new UnsupportedOperationException();
    }

    @Override public TupleEntryIterator openBinaryForRead(WorkflowContext context, String path) throws Throwable {
        throw new UnsupportedOperationException();
    }

    @Override public TupleEntryCollector openBinaryForWrite(WorkflowContext context, String path, String recordName) throws Throwable {
        throw new UnsupportedOperationException();
    }

    @Override public TupleEntryCollector openTextForWrite(WorkflowContext context, String path) throws Throwable {
        throw new UnsupportedOperationException();
    }
}
