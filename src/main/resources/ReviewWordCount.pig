REGISTER yelp-review-0.0.1-SNAPSHOT.jar;
SET mapreduce.output.fileoutputformat.compress true
SET mapreduce.output.fileoutputformat.compress.codec org.apache.hadoop.io.compress.SnappyCodec
SET mapreduce.output.fileoutputformat.compress.type RECORD
SET avro.output.codec snappy
review_json = LOAD '/data/yelp/review/' USING PigStorage() as (json: chararray);
review_with_count = FOREACH review_json GENERATE com.data.challenge.yelp.review.pig.udf.CountNumberOfWordsInReview(*);
review_not_null = FILTER review_with_count BY $0 is NOT NULL;
review_flat = FOREACH review_not_null GENERATE FLATTEN($0) as (user_id: chararray, business_id: chararray,
                        date: chararray, review_id: chararray, starts: double, type: chararray, text: chararray, coolVotes: int, funnyVotes: int, usefulVotes: int,
                        num_words: int);
STORE review_flat INTO '/data/yelp/review-output/' USING AvroStorage();