# Twitter Streaming

In support of digital marketing and analytics for a large, financial services company, we used Twitter data to identify potential marketing targets for different products. This platform was built to collect Tweets given some search criteria and then gather associated user data using the Twitter open API. These data were then written to an AWS database and, later to a Neo4J graph database.

The tweets and associated Twitter user data was then used in different models to:
1. **Identify Life Events**: identifying specific life events that could correspond to increased interest in different products. The expectation was that life events such as birth of a child, graduation from college, or death of a loved one would trigger increased interest in products like life insurance, student loans, or medicare supplements.
2. **Age**: we used user tweets and the language within them to predict the age of the tweeter.
3. **Religious Affiliation**: collecting followers of different religious influencers, churces, etc.. helped to predict the affinity towards these religions, since this was a core value of the organization

The system was deployed and ran continuously, collecting relevant tweets, writing them to the database with metadata identifying what search criteria was used to capture each tweet, and then regularly collecting associated user data. Further analysis to model the events above was done in Jupyter notebooks and other tools.
