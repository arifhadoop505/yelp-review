package com.data.challenge.yelp.review.exceptions;

/**
 * Exception class to be used as an application level exception class for deserialization or serializion. 
 * This class thus helps to maintain unified exception flow of serde's.
 * 
 * @author Arif Mohammad
 */
public class SerializerException extends Exception{

	private static final long serialVersionUID = -6478133188262981219L;
	
	public SerializerException(String message) {
		super(message);
	}
 	public SerializerException(String message, Throwable t){
		super(message, t);
	}
}
