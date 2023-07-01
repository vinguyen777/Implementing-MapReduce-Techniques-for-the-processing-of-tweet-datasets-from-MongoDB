# Implementing MapReduce techniques for the processing of tweet datasets from MongoDB
Twitter serves many objects as JSON, including Tweets and Users. These objects all encapsulate core attributes that describe the object. Each Tweet has an author, a message, a unique ID, a timestamp of when it was posted, and sometimes geo metadata shared by the user. Each User has a Twitter name, an ID, a number of followers, and most often an account bio. With each Tweet, Twitter generates 'entity' objects, which are arrays of common Tweet contents such as hashtags, mentions, media, and links. If there are links, the JSON payload can also provide metadata such as the fully unwound URL and the webpage’s title and description.

To sort a set of tweets from MongoDB on the basis of user IDs and evaluate how relevant a word is to a tweet in a collection
of tweets, Merge Sort, Bucket Sort, and TF-IDF algorithms were implemented using Map-Reduce.

The primary goal of the project is to:

• design, implement and query a NoSQL database.

• implement MapReduce techniques for the processing of Big Data on top of Hadoop (i.e. an open-source version of MapReduce written in Java).
