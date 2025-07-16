from os import listdir
from os.path import isfile, join
import json
import bz2
# from collections import defaultdict
import matplotlib.pyplot as plt

path = "/mnt/erebor1/Daten/ABI2/crawls/dreddit/comments/"

if __name__ == '__main__':
    files = [f for f in listdir(path) if isfile(join(path, f))][:458]
    d = {}
    # text = {}
    # subreddits = defaultdict(int)
    for f in files:
        with bz2.open(path + f, "rt") as file:
            for comment in file:
                comment = json.loads(comment)
                subreddit = comment.get("subreddit")
                user = comment.get("author_fullname")
                # body = comment.get("body")
                if not user in d:
                    d[user] = set([subreddit])
                    # text[user] = [body]
                else:
                    d[user].add(subreddit)
                    # text[user].append(body)
                # subreddits[subreddit] += 1

    with open("user_postings.csv", "w") as file:
        for k, v in d.items():
            if k is not None:
                #print(k ,v)
                file.write(k + "," + ",".join([x for x in v]) + "\n")
    """
    with open("user_texts.csv", "w") as file:
        for k, v in text.items():
            if k is not None:
                file.write(k + "," + ",".join(v) + "\n")
    with open("subreddit_counts.csv", "w") as file:
        s = sorted([(v, k) for k, v in subreddits.items()], reverse=True)
        for v, k in s:
            file.write(str(v) + "," + k + "\n")
        bins = [v for v, _ in s]
        plt.hist(bins, int(bins[0]/1000))
        plt.savefig("subreddit_count_bins.pdf")
    """"     
