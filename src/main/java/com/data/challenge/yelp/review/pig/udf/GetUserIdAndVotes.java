package com.data.challenge.yelp.review.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.data.challenge.yelp.review.beans.Review;
import com.data.challenge.yelp.review.beans.VoteType;
import com.data.challenge.yelp.review.beans.Votes;
import com.data.challenge.yelp.review.exceptions.SerializerException;
import com.data.challenge.yelp.review.serde.Deserializer;
import com.data.challenge.yelp.review.serde.ReviewDeserializer;

public class GetUserIdAndVotes extends EvalFunc<Tuple> {
	
	//TODO Need to add logging for the entire project
	
	private TupleFactory tupleFactory = TupleFactory.getInstance();
	private BagFactory bagFactory = BagFactory.getInstance();
	private Deserializer<Review> reviewDeserializer = new ReviewDeserializer();

	@Override
	public Tuple exec(Tuple input) throws IOException {
		String json = (String) input.get(0);
		Tuple output = tupleFactory.newTuple(2);
		try {

			Review review = reviewDeserializer.deserialize(json);
			String userId = review.getUser_id();
			Votes votes = review.getVotes();

			int coolVotes = votes.getCool();
			int funnyVotes = votes.getFunny();
			int usefulVotes = votes.getUseful();

			DataBag bagOfVotes = bagFactory.newDefaultBag();

			addVotesToBag(VoteType.COOL, coolVotes, bagOfVotes);
			addVotesToBag(VoteType.FUNNY, funnyVotes, bagOfVotes);
			addVotesToBag(VoteType.USEFUL, usefulVotes, bagOfVotes);

			output.set(0, userId);
			output.set(1, bagOfVotes);
			
			return output;

		} catch (SerializerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}

	private void addVotesToBag(VoteType voteType, int coolVotes, DataBag bag)
			throws ExecException {
		Tuple voteTuple = tupleFactory.newTuple(2);
		voteTuple.set(0, voteType.getVoteString());
		voteTuple.set(1, coolVotes);
		bag.add(voteTuple);
	}
	public static void main(String[] args) throws IOException {
		GetUserIdAndVotes review = new GetUserIdAndVotes();
		Tuple input = TupleFactory.getInstance().newTuple(1);
		String json = "{\"votes\": {\"funny\": 0, \"useful\": 0, \"cool\": 0}, \"user_id\": \"PUFPaY9KxDAcGqfsorJp3Q\", \"review_id\": \"Ya85v4eqdd6k9Od8HbQjyA\", \"stars\": 4, \"date\": \"2012-08-01\", \"text\": \"Mr Hoagie is an institution. Walking in, it does seem like a throwback to 30 years ago, old fashioned menu board, booths out of the 70s, and a large selection of food. Their speciality is the Italian Hoagie, and it is voted the best in the area year after year. I usually order the burger, while the patties are obviously cooked from frozen, all of the other ingredients are very fresh. Overall, its a good alternative to Subway, which is down the road.\", \"type\": \"review\", \"business_id\": \"5UmKMjUEUNdYWqANhGckJw\"}\r\n";
		input.set(0, json);
		Tuple output = review.exec(input);
		System.out.println(output);
	}
}
