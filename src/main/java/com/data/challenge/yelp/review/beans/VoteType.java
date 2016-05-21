package com.data.challenge.yelp.review.beans;

public enum VoteType {
	
	COOL("cool"),FUNNY("funny"), USEFUL("useful");
	String voteString;
	private VoteType(String voteString) {
		this.voteString = voteString;
	}
	public String getVoteString(){
		return voteString;
	}
}
