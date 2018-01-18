from Scripts.service import run_topic_continuous

run_topic_continuous(topic_id=9,
                     s3_bucket='aqtwitter',
                     s3_path='tweets/Life Events/Graduation/',
                     tweet_count=1000000)