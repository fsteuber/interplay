import pickle
import numpy as np
from random import shuffle

subreddits = pickle.load(open("subreddits_dict.pcl", "rb"))
users = pickle.load(open("users_dict.pcl", "rb"))
content = pickle.load(open("content_dict.pcl", "rb"))
interest_map_subreddits = [x.strip().split(",") for x in open("interest_map_subreddits.csv").readlines()][0]


def remove_subreddits_not_in_interest_map():
    # remove all unnecessary subreddits
    obsolete_subreddits = []
    obsolete_posts = []
    possibly_obsolete_users = []
    obsolete_users = []

    print("Start of preprocessing: " + str(len(subreddits)) + "," + str(len(users)) + "," + str(len(content)))
    print(interest_map_subreddits)
    for key in subreddits.keys():
        if key not in interest_map_subreddits:
            obsolete_subreddits.append(key)
            for post in subreddits[key]["posts"]:
                obsolete_posts.append(post)
                for user in subreddits[key]["users"]:
                    possibly_obsolete_users.append(user)

    for key in obsolete_subreddits:
        del subreddits[key]
    del obsolete_subreddits

    for key in obsolete_posts:
        if key in content:
            del content[key]

    obsolete_posts = set(obsolete_posts)

    for key in possibly_obsolete_users:
        posts = set(users[key]["posts"]) - obsolete_posts
        if len(list(posts)) == 0:
            obsolete_users.append(key)
        else:
            users[key]["posts"] = list(posts)

    for key in obsolete_users:
        del users[key]

    print("End of preprocessing: " + str(len(subreddits)) + "," + str(len(users)) + "," + str(len(content)))


if __name__ == '__main__':
    remove_subreddits_not_in_interest_map()

    # Training data for average word vectors over all data
    with open("prediction/subreddit_training_data_full.txt", "w") as file:
        for subreddit, sr_dict in subreddits.items():
            for post in sr_dict["posts"]:
                file.write("__label__" + subreddit + " " + content[post]["text"] + "\n")

    # Training data for average word vectors for cross validation
    n_folds = 10
    user_permute = [x for x in users.keys()]  # baseline for cross validation
    shuffle(user_permute)
    user_permute = np.array_split(user_permute, n_folds)

    for i in range(n_folds):
        test_users = []
        train_users = []

        # decide which split goes into which set
        for idx, split in enumerate(user_permute):
            if idx == i:
                for user in split:
                    n_posts = 0
                    for post in users[user]["posts"]:
                        if content[post]["subreddit"] in interest_map_subreddits:
                            n_posts += 1
                        else:
                            continue
                    # only add users to the test set if they have a minimum amount of texts to predict on
                    if n_posts < 10:
                        train_users.append(user)
                    else:
                        test_users.append(user)
            else:
                train_users.extend(split)

        for user in test_users:
            for post in users[user]["posts"]:
                if content[post]["subreddit"] not in interest_map_subreddits:
                    continue

                with open("prediction/subreddit_test_set_fold_" + str(i) + ".txt", "w") as file:
                    file.write("__label__" + content[post]["subreddit"] + " " + content[post]["text"] + "\n")

        for user in train_users:
            for post in users[user]["posts"]:
                if content[post]["subreddit"] not in interest_map_subreddits:
                    continue

                with open("prediction/subreddit_train_set_fold_" + str(i) + ".txt", "w") as file:
                    file.write("__label__" + content[post]["subreddit"] + " " + content[post]["text"] + "\n")


