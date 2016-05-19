package com.data.challenge.yelp.review.serde;

import com.data.challenge.yelp.review.exceptions.SerializerException;

public interface JSONDeserializer<T> {
	public T deserialize(String json) throws SerializerException;
}
