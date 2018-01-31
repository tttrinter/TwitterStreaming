from Scripts.run_topic import run_topic_continuous

run_topic_continuous(topic_id=1,
                     s3_bucket='di-thrivent',
                     s3_path='twitter/Life Events/Graduation/',
                     tweet_count=1000)

