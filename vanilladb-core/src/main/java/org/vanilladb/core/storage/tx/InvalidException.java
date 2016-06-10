package org.vanilladb.core.storage.tx;

/**
 * A runtime exception indicating that the transaction validate failed.
 */
@SuppressWarnings("serial")
public class InvalidException extends RuntimeException{
	public InvalidException() {
	}
	
	public InvalidException(String message) {
		super(message);
	}
}
