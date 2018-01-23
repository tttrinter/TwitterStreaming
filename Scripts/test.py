from Scripts.service import run_topic_continuous

run_topic_continuous(topic_id=9,
                     s3_bucket='di-thrivent',
                     s3_path='twitter/Life Events/Graduation/',
                     tweet_count=10)

