REGISTER yelp-review-0.0.1-SNAPSHOT.jar;
review_json = LOAD '/data/yelp/review/' USING PigStorage() as (json: chararray);
review_with_count = FOREACH review_json GENERATE com.data.challenge.yelp.review.pig.udf.CountNumberOfWordsInReview(*);
review_not_null = FILTER review_with_count BY $0 is NOT NULL;
review_flat = FOREACH review_not_null GENERATE FLATTEN($0);
STORE review_flat INTO '/data/yelp/review-output/';