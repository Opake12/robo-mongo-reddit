import os
import time
import praw
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get environment variables
client_id = os.environ["REDDIT_CLIENT_ID"]
client_secret = os.environ["REDDIT_CLIENT_SECRET"]
#user_agent = os.environ.get("REDDIT_USER_AGENT")


# Initialize Reddit API client
reddit = praw.Reddit(
    client_id=client_id,
    client_secret=client_secret,
    user_agent="fetch-reddit-posts"
)

# Get the top 10 posts from the 'all' subreddit
top_posts = reddit.subreddit("all").hot(limit=10)

mongodb_host = os.environ.get("MONGO_HOST", "localhost")
mongodb_port = os.environ.get("MONGO_PORT", 27018)
mongodb_username = os.environ.get("MONGO_INITDB_ROOT_USERNAME", "root")
mongodb_password = os.environ.get("MONGO_INITDB_ROOT_PASSWORD", "examplepassword")

# Define MongoDB connection string
mongodb_connection_string = f"mongodb://{mongodb_username}:{mongodb_password}@{mongodb_host}:{mongodb_port}/"

# Retry connecting to MongoDB until successful or after a specified number of attempts
max_retries = 5
retry_interval = 5  # seconds

for retry_count in range(1, max_retries + 1):
    try:
        client = MongoClient(mongodb_connection_string, serverSelectionTimeoutMS=5000, socketTimeoutMS=5000)
        client.server_info()  # Test connection
        print("Connected to MongoDB")
        break
    except Exception as e:
        if retry_count < max_retries:
            print(f"Failed to connect to MongoDB. Retrying in {retry_interval} seconds... ({retry_count}/{max_retries})")
            time.sleep(retry_interval)
        else:
            print(f"Failed to connect to MongoDB after {max_retries} attempts. Exiting...")
            raise e

# Create a connection to the 'reddit' database and 'posts' collection
db = client["reddit"]
collection = db["posts"]

# Iterate through the top posts and insert them into the collection
for post in top_posts:
    post_data = {
        "title": post.title,
        "score": post.score,
        "url": post.url,
        "created_utc": post.created_utc
    }
    insert_result = collection.insert_one(post_data)
    print(f"Inserted post with ID: {insert_result.inserted_id}")
