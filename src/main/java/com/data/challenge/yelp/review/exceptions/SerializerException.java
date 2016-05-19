package com.data.challenge.yelp.review.exceptions;

public class SerializerException extends Exception{

	private static final long serialVersionUID = -6478133188262981219L;
	
	public SerializerException(String message) {
		super(message);
	}
 	public SerializerException(String message, Throwable t){
		super(message, t);
	}
}
