package com.scaleunlimited.cascading.cuke;

import cascading.flow.Flow;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

public interface WorkflowInterface {

    /**
     * @param platformName
     * @param parameters
     * @return
     * @throws Exception
     */
    @SuppressWarnings("rawtypes")
    public Flow createFlow(WorkflowPlatform platform, WorkflowParams parameters) throws Throwable;
    
    // public String getDirectoryPath(WorkflowPlatform platformName, String directoryName) throws Exception;
    
    public TupleEntryIterator openBinaryForRead(WorkflowPlatform platformName, WorkflowParams params, String resultsName) throws Throwable;
    
    public TupleEntryCollector openBinaryForWrite(WorkflowPlatform platformName, String directoryName) throws Throwable;
}
