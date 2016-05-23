package com.data.challenge.yelp.review.beans;

/**
 * Bean for holding the review json
 * 
 * @author Arif Mohammad
 */
public class Review {
	
	private String user_id;
	private String review_id;
	private double stars;
	private String date;
	private String text;
	private String type;
	private String business_id;
	private 	Votes votes;

	public String getUser_id() {
		return user_id;
	}
	public void setUser_id(String user_id) {
		this.user_id = user_id;
	}
	public String getReview_id() {
		return review_id;
	}
	public void setReview_id(String review_id) {
		this.review_id = review_id;
	}
	public double getStars() {
		return stars;
	}
	public void setStars(double stars) {
		this.stars = stars;
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public String getText() {
		return text;
	}
	public void setText(String text) {
		this.text = text;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getBusiness_id() {
		return business_id;
	}
	public void setBusiness_id(String business_id) {
		this.business_id = business_id;
	}
	public Votes getVotes() {
		return votes;
	}
	public void setVotes(Votes votes) {
		this.votes = votes;
	}
}
