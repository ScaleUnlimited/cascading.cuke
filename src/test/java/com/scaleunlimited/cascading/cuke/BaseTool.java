package com.scaleunlimited.cascading.cuke;

import com.scaleunlimited.cascading.BasePlatform;
import com.scaleunlimited.cascading.hadoop.HadoopPlatform;
import com.scaleunlimited.cascading.local.LocalPlatform;

public abstract class BaseTool extends BaseWorkflowInterface {

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
