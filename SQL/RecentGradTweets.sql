/* Get recent Graduation tweets from users who are also following at least
one Christian leader and one of Thrivent's colleges */
SELECT DISTINCT users.name, 
				users.screen_name, 
                users.location, 
--                 tweet_id, 
                tweets.text, 
                tweets.created_at, 
                u1.ti_count as christians_followed,
                u2.ti_count as colleges_followed
FROM tweet_scores 
INNER JOIN models on ts_md_id = md_id
INNER JOIN topics on md_tp_id=tp_id
INNER JOIN tweets ON ts_tweet_id = tweet_id
INNER JOIN users ON tweets.user_id=users.id
LEFT OUTER JOIN user_indicator_counts u1 on users.id = u1.ti_user_id AND u1.ti_indicator_id=1
LEFT OUTER JOIN user_indicator_counts u2 on users.id = u2.ti_user_id AND u2.ti_indicator_id=2
WHERE tp_name='Graduation'
AND  u1.ti_count >0
AND u2.ti_count > 0
AND ts_score>0.8
AND tweets.created_at > '2018-03-21'
ORDER BY tweets.created_at desc

