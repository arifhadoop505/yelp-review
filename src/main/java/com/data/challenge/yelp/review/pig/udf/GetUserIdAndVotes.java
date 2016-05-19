package com.data.challenge.yelp.review.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.data.challenge.yelp.review.beans.Review;
import com.data.challenge.yelp.review.beans.Votes;
import com.data.challenge.yelp.review.exceptions.SerializerException;
import com.data.challenge.yelp.review.serde.JSONDeserializer;
import com.data.challenge.yelp.review.serde.ReviewDeserializer;

public class GetUserIdAndVotes extends EvalFunc<Tuple> {
	
	private TupleFactory factory = TupleFactory.getInstance();
	private BagFactory bagFactory = BagFactory.getInstance();
	private JSONDeserializer<Review> reviewDeserializer = new ReviewDeserializer();

	@Override
	public Tuple exec(Tuple input) throws IOException {
		String json = (String) input.get(0);
		Tuple output = factory.newTuple(2);
		try {
			
			Review review = reviewDeserializer.deserialize(json);
			String userId = review.getUser_id();
			Votes votes = review.getVotes();
			
			int coolVotes = votes.getCool();
			int funnyVotes = votes.getFunny();
			int usefulVotes = votes.getUseful();
			
			 DataBag bag = bagFactory.newDefaultBag();
			 
			 addVotesToBag("cool", coolVotes, bag);
			 addVotesToBag("funny", funnyVotes, bag);
			 addVotesToBag("useful", usefulVotes, bag);
			 
			 output.set(0, userId);
			 output.set(1, bag);
			
		} catch (SerializerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}

	private void addVotesToBag( String voteType, int coolVotes, DataBag bag) throws ExecException {
		Tuple voteTuple = factory.newTuple(2);
		voteTuple.set(0, voteType);
		voteTuple.set(1, coolVotes);
		bag.add(voteTuple);
	}
	
}
