package com.data.challenge.yelp.review.pig.udf;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.data.challenge.yelp.review.beans.Review;
import com.data.challenge.yelp.review.exceptions.SerializerException;
import com.data.challenge.yelp.review.serde.Deserializer;
import com.data.challenge.yelp.review.serde.ReviewDeserializer;

/**
 * 
 * UDF to get the number of words for each review and the userid who wrote the review
 * 
 * @author Arif Mohammad
 */
public class GetVerboseReview extends EvalFunc<Tuple> {
	
	private static final Log LOG = LogFactory.getLog(GetVerboseReview.class);

	private TupleFactory tupleFactory = TupleFactory.getInstance();
	private Deserializer<Review> reviewDeserializer = new ReviewDeserializer();
	
	/**
	 * The udf gets the userid and the number of words in the review given a input json tuple
	 * 
	 * @param input Input tuple containing the input json
	 * @return Output tuple containing the userid and number of words in the review.
	 */
	@Override
	public Tuple exec(Tuple input) throws IOException {
		String json = (String) input.get(0);
		Tuple output = tupleFactory.newTuple(2);
		try {
			
			LOG.trace("Processing json: " + json);
			
			Review review = reviewDeserializer.deserialize(json);
			String reviewText = review.getText();
			int numWords = countWords(reviewText);
			
			String userId = review.getUser_id();
			
			output.set(0, userId);
			output.set(1, numWords);
			return output;
		} catch (SerializerException e) {
			
			LOG.error("Exception while deserializing the json: " + json, e);
		} catch(Exception e) {
			
			LOG.error("Unknown exception while processing the json: " + json, e);
		}
		return null;
	}
	
	/**
	 * Method to get the number of words in a review text
	 * 
	 * @param reviewText Review text wrote by the user
	 * @return Number of the words in the review text
	 */
	private int countWords(String reviewText) {
		
		String[] words = reviewText.split("\\s");
		return words.length;
	}
}
