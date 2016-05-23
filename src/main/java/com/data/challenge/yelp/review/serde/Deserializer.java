package com.data.challenge.yelp.review.serde;

import com.data.challenge.yelp.review.exceptions.SerializerException;

/**
 * 
 * Interface to perform deserialization within the application.
 * 
 * @author Arif Mohammad
 * @param <T> The bean to which the given string has to be deserialized to.
 */
public interface Deserializer<T> {
	
	/**
	 * Method to call to deserialize a given string into a bean.
	 * 
	 * @param string The string to deserialize
	 * @return Deserialized bean
	 * @throws SerializerException
	 */
	public T deserialize(String string) throws SerializerException;
}
