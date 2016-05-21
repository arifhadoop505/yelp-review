package com.data.challenge.yelp.review.serde;

import java.io.IOException;

import com.data.challenge.yelp.review.beans.Review;
import com.data.challenge.yelp.review.exceptions.SerializerException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ReviewDeserializer implements Deserializer<Review> {

	public Review deserialize(String json) throws SerializerException {
		ObjectMapper mapper = new ObjectMapper();
		Review review;
		try {
			review = mapper.readValue(json, Review.class);
			return review;
		} catch (JsonParseException e) {
			throw new SerializerException("Unable to parse the json", e);
		} catch (JsonMappingException e) {
			throw new SerializerException("Invalid Json mapping", e);
		} catch (IOException e) {
			throw new SerializerException("Unable to read the provided json", e);
		}
	}
}
