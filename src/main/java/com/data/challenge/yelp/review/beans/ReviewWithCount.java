package com.data.challenge.yelp.review.beans;

public class ReviewWithCount extends Review{
	int count;
	
	public ReviewWithCount(){
		
	}
	public ReviewWithCount(Review review, int count) {
		this.setBusiness_id(review.getBusiness_id());
		this.setCount(count);
		this.setDate(review.getDate());
		this.setReview_id(review.getReview_id());
		this.setStars(review.getStars());
		this.setText(review.getText());
		this.setType(review.getType());
		this.setUser_id(review.getUser_id());
		this.setVotes(review.getVotes());
	}
	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}
}
