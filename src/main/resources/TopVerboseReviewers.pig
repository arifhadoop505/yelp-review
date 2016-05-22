REGISTER yelp-review-0.0.1-SNAPSHOT.jar;
review_json = LOAD '/data/yelp/review/' USING PigStorage() as (json: chararray);
review_verbosity = FOREACH review_json GENERATE FLATTEN(com.data.challenge.yelp.review.pig.udf.GetVerboseReview(*) as (userid: chararray, num_words: int));
review_sorted = ORDER review_verbosity BY num_words;
review_top10 = LIMIT review_sorted 10;
STORE review_top10 INTO '/data/yelp/top_verbose_reviews';