REGISTER yelp-review-0.0.1-SNAPSHOT.jar;
review_json = LOAD '/data/yelp/review/' USING PigStorage() as (json: chararray);
vote_categories = FOREACH review_json GENERATE FLATTEN(com.data.challenge.yelp.review.pig.udf.GetUserIdAndVotes(*)) as (userid: chararray, vote_type: chararray, count: int);
vote_categories_filtered = FILTER vote_categories BY $0 is NOT NULL;
vote_grouped = GROUP vote_categories_filtered BY (userid, vote_type);
vote_group_sum = FOREACH vote_grouped GENERATE FLATTEN($0), SUM($1.count);

cool_votes = FILTER vote_group_sum BY $1 == 'cool';
cool_votes_sort = ORDER cool_votes BY $2 DESC;
cool_top10 = LIMIT cool_votes_sort 10;

funny_votes = FILTER vote_group_sum BY $1 == 'funny';
funny_votes_sort = ORDER funny_votes BY $2 DESC;
funny_top10 = LIMIT funny_votes_sort 10;

useful_votes = FILTER vote_group_sum BY $1 == 'useful';
useful_votes_sort = ORDER useful_votes BY $2 DESC;
useful_top10 = LIMIT useful_votes_sort 10;

STORE cool_top10 INTO '/data/yelp/cool-dashboard/';
STORE funny_top10 INTO '/data/yelp/funny-dashboard/';
STORE useful_top10 INTO '/data/yelp/useful-dashboard/';