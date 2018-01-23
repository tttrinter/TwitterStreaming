import boto3
import os
from time import gmtime, strftime
from Streaming.Topic import Topic
from Streaming.Streamer import TwitterStream
import sys

def run_topic_continuous(topic_id: int, s3_bucket: str, s3_path: str, tweet_count=1000):
    """
    1. Create the topic and stream
    2. Collects tweet_count tweets
    3. Save tweet file to S3 when finished
    """

    # 1. Create the topic and stream
    run_topic = Topic(topic_id=topic_id)
    run_topic.readTopic()
    if run_topic.name is None:
        print("Topic not found")
        quit()

    # Temporary output file

    # 2. Start Twitter stream running
    print("Starting {} stream for {} tweets.".format(run_topic.name, tweet_count))
    iteration = 0
    boto3.setup_default_session(profile_name='di')
    s3 = boto3.resource('s3')
    save_bucket = s3_bucket

    while True:
        iteration += 1
        outfile = run_topic.name + "_" + strftime("%Y%m%d%H%M%S", gmtime()) + ".json"
        run_stream = TwitterStream(name=run_topic.name, topic=run_topic, outfile=outfile)
        run_stream.startStream(tweet_count=tweet_count, async=True)

        # 3. Save file to S3
        print("Saving {} to {}.".format(outfile, s3_path))
        key = s3_path + outfile
        try:
            s3.meta.client.upload_file(outfile, save_bucket, key)
        except Exception as e:
            print(e)

        # 4. Delete the temporary file
        os.remove(outfile)


if __name__ == "__main__":
    print("in Main")
    topic_id = int(sys.argv[1])
    s3_bucket = sys.argv[2]
    s3_path = sys.argv[3]
    tweet_count = sys.argv[4]
    print('Running: Topic id: {}, s3 bucket: {}, s3 path: {}, tweet count: {}'.format(topic_id, s3_bucket, s3_path, tweet_count))

    run_topic_continuous(topic_id, s3_bucket,s3_path,tweet_count)
