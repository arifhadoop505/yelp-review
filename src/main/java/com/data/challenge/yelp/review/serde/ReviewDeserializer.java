package com.data.challenge.yelp.review.serde;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.data.challenge.yelp.review.beans.Review;
import com.data.challenge.yelp.review.exceptions.SerializerException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 
 * Class to deserialize the given review json to Review bean represented by Review.java
 * 
 * @author Arif Mohammad
 *
 */
public class ReviewDeserializer implements Deserializer<Review> {

	private static final Log LOG = LogFactory.getLog(ReviewDeserializer.class);

	public Review deserialize(String json) throws SerializerException {
		ObjectMapper mapper = new ObjectMapper();
		Review review;
		try {
			
			LOG.trace("Deserializing the json: " + json);
			review = mapper.readValue(json, Review.class);
			return review;
		} catch (JsonParseException e) {
			
			LOG.error("Unable to parse the json:" + json, e);
			throw new SerializerException("Unable to parse the json", e);
		} catch (JsonMappingException e) {
			
			LOG.error("Invalid json mapping exception while processing the json: " + json, e);
			throw new SerializerException("Invalid Json mapping", e);
		} catch (IOException e) {
			
			LOG.error("Unable to the read the provided json: " + json, e);
			throw new SerializerException("Unable to read the provided json", e);
		}
	}
}
