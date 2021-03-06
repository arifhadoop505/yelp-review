package com.data.challenge.yelp.review.beans;

/**
 * Bean for holding the Votes nested json with review json
 * 
 * @author Arif Mohammad
 */
public class Votes {
	
	private int funny;
	private int useful;
	private int cool;
	
	public int getFunny() {
		return funny;
	}
	public void setFunny(int funny) {
		this.funny = funny;
	}
	public int getUseful() {
		return useful;
	}
	public void setUseful(int useful) {
		this.useful = useful;
	}
	public int getCool() {
		return cool;
	}
	public void setCool(int cool) {
		this.cool = cool;
	}
}
