import praw
import queue
import threading
import time
import os
import pandas as pd

reddit = praw.Reddit(
    client_id="",
    client_secret="",
    user_agent=""
)

subreddits = [
    "cryptocurrency", "bitcoin", "ethereum", "CryptoMarkets", "altcoin",
    "defi", "cryptotrading", "binance", "cardano", "solana",
    "nft", "cryptomoonshots", "bitcoinbeginners", "cryptocurrencies", "ethtrader"
]

batch_size = 5000
output_file = "crypto_reddit_data_unlimited.csv"
data_queue = queue.Queue()

def scrape_posts(subreddit_name):
    try:
        subreddit = reddit.subreddit(subreddit_name)
        post_count = 0
        for post in subreddit.top(limit=None):
            try:
                author_name = "[deleted]"
                if post.author:
                    try:
                        author_name = post.author.name
                    except AttributeError:
                        pass
                data_queue.put([
                    post.id,
                    author_name,
                    post.title,
                    post.selftext,
                    post.score,
                    post.created_utc,
                    "post",
                    post.id,
                    subreddit_name
                ])
                post_count += 1
                if post_count % 10 == 0:
                    print(f"Scraped {post_count} posts from {subreddit_name}...")
                try:
                    post.comments.replace_more(limit=0)
                    comment_count = 0
                    for comment in post.comments.list():
                        try:
                            comment_author = "[deleted]"
                            if comment.author:
                                try:
                                    comment_author = comment.author.name
                                except AttributeError:
                                    pass
                            data_queue.put([
                                comment.id,
                                comment_author,
                                post.title,
                                comment.body,
                                comment.score,
                                comment.created_utc,
                                "comment",
                                post.id,
                                subreddit_name
                            ])
                            comment_count += 1
                            if comment_count % 100 == 0:
                                print(f"Scraped {comment_count} comments from post {post.id} in {subreddit_name}...")
                        except Exception as e:
                            print(f"Error processing comment: {str(e)}")
                except Exception as e:
                    print(f"Error processing comments for post {post.id}: {str(e)}")
                time.sleep(1)
            except Exception as e:
                print(f"Error processing post: {str(e)}")
                time.sleep(2)
    except Exception as e:
        print(f"Error scraping subreddit {subreddit_name}: {str(e)}")

def data_saver():
    current_batch = []
    while True:
        try:
            data = data_queue.get(timeout=5)
            current_batch.append(data)
            data_queue.task_done()
            if len(current_batch) >= batch_size:
                save_batch(current_batch)
                current_batch = []
        except queue.Empty:
            continue

def save_batch(batch):
    if not batch:
        return
    try:
        df = pd.DataFrame(batch, columns=["id", "user", "title", "text", "score", "created_utc", "type", "post_id", "subreddit"])
        if not os.path.exists(output_file):
            df.to_csv(output_file, index=False)
        else:
            df.to_csv(output_file, mode='a', header=False, index=False)
        print(f"Saved batch of {len(batch)} entries.")
    except Exception as e:
        print(f"Error saving data: {str(e)}")

if __name__ == "__main__":
    saver_thread = threading.Thread(target=data_saver, daemon=True)
    saver_thread.start()
    scraper_threads = []
    for subreddit in subreddits:
        thread = threading.Thread(target=scrape_posts, args=(subreddit,), daemon=True)
        scraper_threads.append(thread)
        thread.start()
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        print("\nInterrupted by user. Saving remaining data before exiting...")
        save_batch(list(data_queue.queue))
        print("Scraping stopped.")
