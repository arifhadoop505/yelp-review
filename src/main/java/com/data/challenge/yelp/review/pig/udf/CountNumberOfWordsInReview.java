package com.data.challenge.yelp.review.pig.udf;

import java.io.IOException;
import java.io.StringWriter;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.data.challenge.yelp.review.beans.Review;
import com.data.challenge.yelp.review.beans.ReviewWithCount;
import com.data.challenge.yelp.review.exceptions.SerializerException;
import com.data.challenge.yelp.review.serde.Deserializer;
import com.data.challenge.yelp.review.serde.ReviewDeserializer;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CountNumberOfWordsInReview extends EvalFunc<Tuple> {
	
	private TupleFactory factory = TupleFactory.getInstance();
	private Deserializer<Review> reviewDeserializer = new ReviewDeserializer();
	
	@Override
	public Tuple exec(Tuple input) throws IOException {
		String json = (String) input.get(0);
		try {
			
			Review review = reviewDeserializer.deserialize(json);
			int numWords = countWords(review);
			Tuple output = getOutputTuple(review, numWords);
			
			return output;
		} catch (SerializerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	private Tuple getOutputTuple(Review review, int numWords) throws JsonGenerationException, JsonMappingException, IOException {
		Tuple output = factory.newTuple(2);
		ReviewWithCount rev2 = new ReviewWithCount(review, numWords);
		ObjectMapper mapper = new ObjectMapper();
		StringWriter writer = new StringWriter();
		mapper.writeValue(writer, rev2);
		output.set(0, writer.toString());
		return output;
	}

	private int countWords(Review review) {
		
		String reviewText = review.getText();
		String[] words = reviewText.split("\\s");
		return words.length;
	}
	public static void main(String[] args) throws IOException {
		CountNumberOfWordsInReview review = new CountNumberOfWordsInReview();
		Tuple input = TupleFactory.getInstance().newTuple(1);
		String json = "{\"votes\": {\"funny\": 0, \"useful\": 0, \"cool\": 0}, \"user_id\": \"PUFPaY9KxDAcGqfsorJp3Q\", \"review_id\": \"Ya85v4eqdd6k9Od8HbQjyA\", \"stars\": 4, \"date\": \"2012-08-01\", \"text\": \"Mr Hoagie is an institution. Walking in, it does seem like a throwback to 30 years ago, old fashioned menu board, booths out of the 70s, and a large selection of food. Their speciality is the Italian Hoagie, and it is voted the best in the area year after year. I usually order the burger, while the patties are obviously cooked from frozen, all of the other ingredients are very fresh. Overall, its a good alternative to Subway, which is down the road.\", \"type\": \"review\", \"business_id\": \"5UmKMjUEUNdYWqANhGckJw\"}\r\n";
		input.set(0, json);
		Tuple output = review.exec(input);
		System.out.println(output.toDelimitedString("\t"));
	}
}
