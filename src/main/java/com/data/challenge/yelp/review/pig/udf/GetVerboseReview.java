package com.data.challenge.yelp.review.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.data.challenge.yelp.review.beans.Review;
import com.data.challenge.yelp.review.exceptions.SerializerException;
import com.data.challenge.yelp.review.serde.Deserializer;
import com.data.challenge.yelp.review.serde.ReviewDeserializer;

public class GetVerboseReview extends EvalFunc<Tuple> {
	
	private TupleFactory tupleFactory = TupleFactory.getInstance();
	private Deserializer<Review> reviewDeserializer = new ReviewDeserializer();
	
	@Override
	public Tuple exec(Tuple input) throws IOException {
		String json = (String) input.get(0);
		Tuple output = tupleFactory.newTuple(2);
		try {
			Review review = reviewDeserializer.deserialize(json);
			String reviewText = review.getText();
			int numWords = countWords(reviewText);
			
			String userId = review.getUser_id();
			
			output.set(0, userId);
			output.set(1, numWords);
			return output;
		} catch (SerializerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	private int countWords(String reviewText) {
		
		String[] words = reviewText.split("\\s");
		return words.length;
	}
}
