package com.data.challenge.yelp.review.pig.udf;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.data.challenge.yelp.review.beans.Review;
import com.data.challenge.yelp.review.exceptions.SerializerException;
import com.data.challenge.yelp.review.serde.Deserializer;
import com.data.challenge.yelp.review.serde.ReviewDeserializer;

/**
 * Pig UDF for counting and reporting the number of words in a review. The udf also deserializes 
 * and returns the review text in a tuple.
 * 
 * @author Arif Mohammad
 */
public class CountNumberOfWordsInReview extends EvalFunc<Tuple> {
	
	private static final Log LOG = LogFactory.getLog(CountNumberOfWordsInReview.class);

	private TupleFactory factory = TupleFactory.getInstance();
	private Deserializer<Review> reviewDeserializer = new ReviewDeserializer();
	
	@Override
	public Tuple exec(Tuple input) throws IOException {
		String json = (String) input.get(0);
		try {
			
			LOG.trace("Processing json object: " + json);
			
			Review review = reviewDeserializer.deserialize(json);
			int numWords = countWords(review);
			
			Tuple output = getOutputTuple(review, numWords);
			
			LOG.trace("Sucessfully processed the json object: " + json + 
					" ,Number of words in the json: " + numWords);
			
			return output;
		} catch (SerializerException e) {
			
			LOG.error("Exception deserializing the input json: " + json, e);
		} catch(Exception e) {
			
			LOG.error("Unknown exception while processing the json: " + json, e);
		}
		return null;
	}

	/** 
	 * 
	 * This methods takes in the review bean and the number of words of review text and 
	 * returns a flattened tuple as output
	 * 
	 * @param review The deserialized review bean
	 * @param numWords The number of words counted within the review text bean.
	 * @return Tuple Tuple containing the fields in the review as well as the number of words
	 * 		   as in numWords parameter
	 * @throws ExecException
	 */
	private Tuple getOutputTuple(Review review, int numWords)
			throws ExecException {
		Tuple output = factory.newTuple(11);
		output.set(0, review.getUser_id());
		output.set(1, review.getBusiness_id());
		output.set(2, review.getDate());
		output.set(3, review.getReview_id());
		output.set(4, review.getStars());
		output.set(5, review.getType());
		output.set(6, review.getText());
		output.set(7, review.getVotes().getCool());
		output.set(8, review.getVotes().getFunny());
		output.set(9, review.getVotes().getUseful());
		output.set(10, numWords);
		return output;
	}

	/**
	 * 
	 * Private method to count the number of words in a review.
	 * @param review Deserialized review object
	 * @return Number of words within the <quote>text</quote> field of the review object
	 */
	private int countWords(Review review) {
		
		String reviewText = review.getText();
		String[] words = reviewText.split("\\s");
		return words.length;
	}
	
	/**
	 * 
	 * Driver function just to test the udf.
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		CountNumberOfWordsInReview review = new CountNumberOfWordsInReview();
		Tuple input = TupleFactory.getInstance().newTuple(1);
		String json = "{\"votes\": {\"funny\": 0, \"useful\": 0, \"cool\": 0}, \"user_id\": \"PUFPaY9KxDAcGqfsorJp3Q\", \"review_id\": \"Ya85v4eqdd6k9Od8HbQjyA\", \"stars\": 4, \"date\": \"2012-08-01\", \"text\": \"Mr Hoagie is an institution. Walking in, it does seem like a throwback to 30 years ago, old fashioned menu board, booths out of the 70s, and a large selection of food. Their speciality is the Italian Hoagie, and it is voted the best in the area year after year. I usually order the burger, while the patties are obviously cooked from frozen, all of the other ingredients are very fresh. Overall, its a good alternative to Subway, which is down the road.\", \"type\": \"review\", \"business_id\": \"5UmKMjUEUNdYWqANhGckJw\"}\r\n";
		input.set(0, json);
		Tuple output = review.exec(input);
		System.out.println(output.toDelimitedString("\t"));
	}
}
