from os import listdir
from os.path import isfile, join
import bz2
from collections import defaultdict
from multiprocessing import Queue, Process
import json

path_comments = "/mnt/erebor1/Daten/ABI2/crawls/dreddit/comments/"
path_submissions = "/mnt/erebor1/Daten/ABI2/crawls/dreddit/submissions/"

# all files until (including) 2020-03-20
n_comments = 458
n_submissions = 246
n_cores = 45  # worker processes
min_comments = 500  # comment threshold
min_submissions = 250  # submission threshold


def subreddits_with_min_comments():
    def run_comments(qw, qm):  # worker jobs
        subreddits_worker = defaultdict(int)
        has_work = True
        while has_work:
            next = qw.get()
            if next is None:  # if no file is left to be processed, a None will be popped as exit criterion
                has_work = False
                continue
            else:
                with bz2.open(path_comments + f, "rt") as file:
                    for comment in file:
                        comment = json.loads(comment)
                        subreddit = comment.get("subreddit")
                        subreddits_worker[subreddit] += 1
        qm.put(subreddits_worker)
        qm.put(None)  # once finished, each worker sends a None and joins

    # command channels
    q_worker = Queue(n_comments + n_cores)
    q_master = Queue(n_cores * 2)

    # worker processes
    comment_workers = [Process(target=run_comments, args=(q_worker, q_master)) for _ in range(n_cores)]

    # create work batches
    files = [f for f in listdir(path_comments) if isfile(join(path_comments, f))][:n_comments]
    for f in files:  # push all file names as jobs onto the queue
        q_worker.put(f)
    for _ in comment_workers:  # push None as exit signal for each worker process
        q_worker.put(None)

    for w in comment_workers:
        w.start()

    # collect and combine partial results from workers
    subreddits = defaultdict(int)
    remaining_nones = n_cores  # represents the number of workers that have to finish their computations

    while remaining_nones > 0:
        partial_result = q_master.get()

        if partial_result is None:  # signal that a worker has finished
            remaining_nones -= 1
            continue
        else:  # got a dictionary containing subreddit counts
            for k, v in partial_result.items():
                subreddits[k] += v

    # filter on subreddits which have min_comments+ comments
    subreddits = sorted([(v, k) for k, v in subreddits.items() if v >= min_comments], reverse=True)
    return [(k, v) for v, k in subreddits]


def subreddits_with_min_submissions():
    def run_submissions(qw, qm):  # worker jobs
        subreddits_worker = defaultdict(int)
        has_work = True
        while has_work:
            next = qw.get()
            if next is None:  # if no file is left to be processed, a None will be popped as exit criterion
                has_work = False
                continue
            else:
                with bz2.open(path_submissions + f, "rt") as file:
                    for submission in file:
                        submission = json.loads(submission)
                        subreddit = submission.get("subreddit")
                        subreddits_worker[subreddit] += 1
        qm.put(subreddits_worker)
        qm.put(None)  # once finished, each worker sends a None and joins

    # command channels
    q_worker = Queue(n_submissions + n_cores)
    q_master = Queue(n_cores * 2)

    # worker processes
    submission_workers = [Process(target=run_submissions, args=(q_worker, q_master)) for _ in range(n_cores)]

    # create work batches
    files = [f for f in listdir(path_submissions) if isfile(join(path_submissions, f))][:n_submissions]
    for f in files:  # push all file names as jobs onto the queue
        q_worker.put(f)
    for _ in submission_workers:  # push None as exit signal for each worker process
        q_worker.put(None)

    for w in submission_workers:
        w.start()

    # collect and combine partial results from workers
    subreddits = defaultdict(int)
    remaining_nones = n_cores  # represents the number of workers that have to finish their computations

    while remaining_nones > 0:
        partial_result = q_master.get()

        if partial_result is None:  # signal that a worker has finished
            remaining_nones -= 1
            continue
        else:  # got a dictionary containing subreddit counts
            for k, v in partial_result.items():
                subreddits[k] += v

    # filter on subreddits which have min_submissions+ submissions
    subreddits = sorted([(v, k) for k, v in subreddits.items() if v >= min_submissions], reverse=True)
    return [(k, v) for v, k in subreddits]


if __name__ == '__main__':
    # 1. Iterate over all comment files and filter on common subreddits
    s = subreddits_with_min_comments()

    with open("comment_counts.csv", "w") as file:
        for k, v in s:
            file.write(str(v) + "," + k + "\n")

    relevant_subreddits = set([k for k, _ in s])
    
    # 2. Iterate over all submission files and filter on common subreddits
    s = subreddits_with_min_submissions()

    with open("submission_counts.csv", "w") as file:
        for k, v in s:
            file.write(str(v) + "," + k + "\n")

    relevant_subreddits = relevant_subreddits.union(set([k for k, _ in s]))

    with open("relevant_subreddits.txt", "w") as file:
        file.write(",".join(list(relevant_subreddits)))

