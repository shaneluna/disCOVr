# #1.1 users who send out most biased tweets
match (u:User)-[:tweeted]->(:Tweet)-[:cited]->(a:Article)
where a.bias_content_decision = 'bias'
return u.name, u.username, u.verified, count(*) as bias_cnt
order by bias_cnt desc

# #1.2 users who send out most impartial tweets
match (u:User)-[:tweeted]->(:Tweet)-[:cited]->(a:Article)
where a.bias_content_decision = 'impartial'
return u.name, u.username, u.verified, count(*) as bias_cnt
order by bias_cnt desc

# #1.3 tweet counts based on verified status and bias
match (u:User)-[:tweeted]->(t:Tweet)-[:cited]->(a:Article)
return
    a.bias_content_decision as bias_content_decision,
    u.verified as verified,
    count(t) as count_tweets
order by bias_content_decision, verified

# #2 impact of bias on reactions
match (u:User)-[:tweeted]->(t:Tweet)-[:cited]->(a:Article)
return
    a.bias_content_decision,
    count(*) as count_tweets,
    avg(t.public_metrics_like_count) as avg_likes,
    max(t.public_metrics_like_count) as max_likes,
    min(t.public_metrics_like_count) as min_likes,
    stDev(t.public_metrics_like_count) as std_likes,
    avg(t.public_metrics_reply_count) as avg_replies,
    max(t.public_metrics_reply_count) as max_replies,
    min(t.public_metrics_reply_count) as min_replies,
    stDev(t.public_metrics_reply_count) as std_replies,
    avg(t.public_metrics_retweet_count) as avg_retweets,
    max(t.public_metrics_retweet_count) as max_retweets,
    min(t.public_metrics_retweet_count) as min_retweets,
    stDev(t.public_metrics_retweet_count) as std_retweets

