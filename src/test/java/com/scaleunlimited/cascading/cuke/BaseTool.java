package com.scaleunlimited.cascading.cuke;

import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.local.LocalFlowProcess;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;

import com.scaleunlimited.cascading.BasePlatform;
import com.scaleunlimited.cascading.hadoop.HadoopPlatform;
import com.scaleunlimited.cascading.local.KryoScheme;
import com.scaleunlimited.cascading.local.LocalPlatform;

public abstract class BaseTool {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	
    protected BasePlatform makePlatform(WorkflowPlatform platform) {
        if (platform == WorkflowPlatform.DISTRIBUTED) {
            return new HadoopPlatform(this.getClass());
        } else if (platform == WorkflowPlatform.LOCAL) {
            return new LocalPlatform(this.getClass());
        } else {
            throw new IllegalArgumentException("Unknown platform: " + platform);
        }
    }


}
