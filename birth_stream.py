from Scripts.run_topic import run_topic_continuous

run_topic_continuous(topic_id=2,
                     s3_bucket='di-thrivent',
                     s3_path='twitter/Life Events/Birth/',
                     tweet_count=1000)

