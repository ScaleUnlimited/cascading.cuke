package com.scaleunlimited.cascading.cuke;

@SuppressWarnings("serial")
public class StepFailureException extends Exception {

	public StepFailureException() {
		super();
	}

	public StepFailureException(String message, Throwable cause) {
		super(message, cause);
	}

	public StepFailureException(String message) {
		super(message);
	}

	public StepFailureException(Throwable cause) {
		super(cause);
	}
	
}
