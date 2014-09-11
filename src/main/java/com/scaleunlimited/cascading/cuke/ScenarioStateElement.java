package com.scaleunlimited.cascading.cuke;

import java.io.Closeable;

/**
 * An individual piece of data saved in the {@link ScenarioState} and accessed
 * via a unique key.
 * 
 * TODO This should probably be a generic type instead of an interface.
 */
public interface ScenarioStateElement extends Closeable {

}
