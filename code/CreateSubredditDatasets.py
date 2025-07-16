from os import listdir
from os.path import isfile, join
import bz2
from multiprocessing import Queue, Process
import json
import pickle

path_comments = "/mnt/erebor1/Daten/ABI2/crawls/dreddit/comments/"
path_submissions = "/mnt/erebor1/Daten/ABI2/crawls/dreddit/submissions/"

# all files until (including) 2020-03-20
n_comments = 458
n_submissions = 246
n_cores = 45  # worker processes
min_comments = 500  # comment threshold
min_submissions = 250  # submission threshold
min_words_per_post = 5

relevant_subreddits = []


def build_datasets():
    global relevant_subreddits

    def run(qw, qm):  # worker jobs
        subreddits_worker = {}
        user_worker = {}
        content_worker = {}

        has_work = True
        while has_work:
            next, type = qw.get()
            if next is None:  # if no file is left to be processed, a None will be popped as exit criterion
                has_work = False
                continue
            else:
                with bz2.open(next, "rt") as file:
                    for post in file:
                        # extract necessary information
                        post = json.loads(post)
                        subreddit = post.get("subreddit")
                        post_id = post.get("id")
                        user = post.get("author_fullname")

                        if type == "c":
                            content = {"text": post.get("body"), "subreddit": subreddit}
                        elif type == "s":
                            content = {"text": post.get("selftext"), "subreddit": subreddit}

                        # work on filtered subreddits only
                        if subreddit not in relevant_subreddits:
                            continue

                        # skip all entries that have less then the minimal required number of words (e.g. picture posts)
                        if len(content["text"].split(" ")) < min_words_per_post:
                            continue

                        # fill maps
                        if subreddit not in subreddits_worker:
                            subreddits_worker[subreddit] = {
                                "posts": [post_id],
                                "users": set([user])
                            }
                        else:
                            subreddits_worker[subreddit]["posts"].append(post_id)
                            subreddits_worker[subreddit]["users"].add(user)

                        if user not in user_worker:
                            user_worker[user] = {
                                "posts": [post_id],
                                "subreddits": set([subreddit])
                            }
                        else:
                            user_worker[user]["posts"].append(post_id)
                            user_worker[user]["subreddits"].add(subreddit)

                        content_worker[post_id] = content

        qm.put((subreddits_worker, user_worker, content_worker))
        qm.put((None, None, None))  # once finished, each worker sends a None and joins

    # command channels
    q_worker = Queue(n_comments + n_submissions + n_cores)
    q_master = Queue(n_cores * 2)

    # worker processes
    workers = [Process(target=run, args=(q_worker, q_master)) for _ in range(n_cores)]

    # create work batches for comment files
    files = [f for f in listdir(path_comments) if isfile(join(path_comments, f))][:n_comments]
    for f in files:  # push all file names as jobs onto the queue
        q_worker.put((path_comments + f, "c"))

    # create work batches for submission files
    files = [f for f in listdir(path_submissions) if isfile(join(path_submissions, f))][:n_submissions]
    for f in files:  # push all file names as jobs onto the queue
        q_worker.put((path_submissions + f, "s"))

    # push None as exit signal for each worker process
    for _ in workers:
        q_worker.put((None, None))

    for w in workers:
        w.start()

    # final dictionaries
    subreddits = {}
    users = {}
    content = {}

    # collect and combine partial results from workers
    remaining_nones = n_cores  # represents the number of workers that have to finish their computations

    while remaining_nones > 0:
        s, u, c = q_master.get()

        if s is None:  # signal that a worker has finished
            remaining_nones -= 1
            continue
        else:  # got a dictionary
            # merge subreddits dictionaries
            for k, v in s.items():
                if k not in subreddits:
                    subreddits[k] = {
                        "posts": v["posts"],
                        "users": v["users"]
                    }
                else:
                    subreddits[k]["posts"].extend(v["posts"])
                    subreddits[k]["users"].union(v["users"])

            # merge users dictionaries
            for k, v in u.items():
                if k not in users:
                    users[k] = {
                        "posts": v["posts"],
                        "subreddits": v["subreddits"]
                    }
                else:
                    users[k]["posts"].extend(v["posts"])
                    users[k]["subreddits"].union(v["subreddits"])

            # complete content dictionary
            for k, v in c.items():
                content[k] = v

    del workers  # remove zombies before writing results

    print("serializing results")
    pickle.dump(subreddits, open("subreddits_dict.pcl", "wb"))
    pickle.dump(users, open("users_dict.pcl", "wb"))
    pickle.dump(content, open("content_dict.pcl", "wb"))

    # create user_posting list
    with open("user_postings.csv", "w") as file:
        for user, user_dict in users.items():
            if user is None or user_dict["subreddits"] is None:
                continue
            file.write(user + "," + ",".join(list(user_dict["subreddits"])) + "\n")


if __name__ == '__main__':
    relevant_subreddits = open("relevant_subreddits.txt").readlines()[0].strip().split(",")

    # debug message to ensure the fileio is working
    print("extracting posts for " + str(len(relevant_subreddits)) + " subreddits")

    build_datasets()

