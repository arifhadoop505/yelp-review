package com.data.challenge.yelp.review.pig.udf;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

/**
 * Pig UDF for getting different types of votes and their count in a review.
 * 
 * @author Arif Mohammad
 */
public class GetUserIdAndVotes extends EvalFunc<Tuple> {
	
	private static final Log LOG = LogFactory.getLog(GetUserIdAndVotes.class);
	
	private TupleFactory tupleFactory = TupleFactory.getInstance();
	private BagFactory bagFactory = BagFactory.getInstance();
	private Deserializer<Review> reviewDeserializer = new ReviewDeserializer();
	/**
	 * This udf deserializes the json and gets user id and a bag of vote tuples. Each vote tuple will
	 * contain vote_type and the number of votes for that vote_type.
	 * 
	 * @param input Input tuple containing the review json
	 * @return Returns a tuple containing 2 fields. First field is user_id and the second field
	 *		 	is a bag of vote tuples. Each vote tuple contains 2 fields - vote_type and 
	 *		    number of votes for that tuple
	 */
	@Override
	public Tuple exec(Tuple input) throws IOException {
		String json = (String) input.get(0);
		Tuple output = tupleFactory.newTuple(2);
		try {
			
			LOG.trace("Processing json: " + json);
			
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
			
			LOG.error("Exception while deserializing the json: " + json, e);
		}
		catch(Exception e) {
			
			LOG.error("Unknown exception while processing the json: " + json, e);
		}

		return null;
	}
	/**
	 * 
	 * Adds a vote tuple to a bag.
	 * 
	 * @param voteType Type of the vote described by VoteType enum
	 * @param numVotes Number of votes corresponding the vote type
	 * @param bag Bag to add the vote tuple to.
	 * @throws ExecException
	 */
	private void addVotesToBag(VoteType voteType, int numVotes, DataBag bag)
			throws ExecException {
		Tuple voteTuple = tupleFactory.newTuple(2);
		voteTuple.set(0, voteType.getVoteString());
		voteTuple.set(1, numVotes);
		bag.add(voteTuple);
	}
	
	/**
	 * Driver method just to test the udf
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		GetUserIdAndVotes review = new GetUserIdAndVotes();
		Tuple input = TupleFactory.getInstance().newTuple(1);
		String json = "{\"votes\": {\"funny\": 0, \"useful\": 0, \"cool\": 0}, \"user_id\": \"PUFPaY9KxDAcGqfsorJp3Q\", \"review_id\": \"Ya85v4eqdd6k9Od8HbQjyA\", \"stars\": 4, \"date\": \"2012-08-01\", \"text\": \"Mr Hoagie is an institution. Walking in, it does seem like a throwback to 30 years ago, old fashioned menu board, booths out of the 70s, and a large selection of food. Their speciality is the Italian Hoagie, and it is voted the best in the area year after year. I usually order the burger, while the patties are obviously cooked from frozen, all of the other ingredients are very fresh. Overall, its a good alternative to Subway, which is down the road.\", \"type\": \"review\", \"business_id\": \"5UmKMjUEUNdYWqANhGckJw\"}\r\n";
		input.set(0, json);
		Tuple output = review.exec(input);
		System.out.println(output);
	}
}
