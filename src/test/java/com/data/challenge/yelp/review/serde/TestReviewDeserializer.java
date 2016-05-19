package com.data.challenge.yelp.review.serde;

import org.junit.Assert;
import org.junit.Test;

import com.data.challenge.yelp.review.beans.Review;
import com.data.challenge.yelp.review.exceptions.SerializerException;

public class TestReviewDeserializer {
	@Test
	public void deserialize() throws SerializerException {
		String json = "{\"votes\": {\"funny\": 0, \"useful\": 0, \"cool\": 0}, \"user_id\": \"PUFPaY9KxDAcGqfsorJp3Q\", \"review_id\": \"Ya85v4eqdd6k9Od8HbQjyA\", \"stars\": 4, \"date\": \"2012-08-01\", \"text\": \"Mr Hoagie is an institution. Walking in, it does seem like a throwback to 30 years ago, old fashioned menu board, booths out of the 70s, and a large selection of food. Their speciality is the Italian Hoagie, and it is voted the best in the area year after year. I usually order the burger, while the patties are obviously cooked from frozen, all of the other ingredients are very fresh. Overall, its a good alternative to Subway, which is down the road.\", \"type\": \"review\", \"business_id\": \"5UmKMjUEUNdYWqANhGckJw\"}\r\n";
		JSONDeserializer<Review> deserializer = new ReviewDeserializer();

		Review review = deserializer.deserialize(json);
		Assert.assertNotNull(review);
	}
	@Test(expected=SerializerException.class)
	public void deserialize2() throws SerializerException {
		String json = "Dummy";
		JSONDeserializer<Review> deserializer = new ReviewDeserializer();
		deserializer.deserialize(json);
	}
}
