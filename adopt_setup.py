import pandas as pd
import numpy as np
import json
import re

def open_reddit_json(file_path):
    """
    Takes a string of a json (of scrapped Reddit data) file and turns it into a dataframe
    Inputs:
        file_path (str): the file path of the JSON file
    Returns pandas DataFrame of pertinent information
    """
    file = open(file_path, "r")

    users = []
    user_flairs = []
    titles = []
    post_texts = []
    post_dates = []
    post_flairs = []
    scores = []
    n_comments_list = []
    #links = []

    for line in file:
        post = json.loads(line)

        try:
            users.append(post.get("author", np.nan))
            user_flairs.append(post.get("author_flair_text", np.nan))
            titles.append(post.get("title", np.nan)) # will be nan for comments
            
            # post text data is located in different places if comment vs post
            post_text = post.get("selftext")
            if post_text:
                post_texts.append(post_text)
            else:
                post_texts.append(post.get("body", np.nan))
            post_dates.append(post.get("created_utc", np.nan))
            post_flairs.append(post.get("link_flair_text", np.nan))
            scores.append(post.get("score", np.nan))
            n_comments_list.append(post.get("num_comments", np.nan))
            #links.append(post.get("url", np.nan))
            
        except:
            continue

    output = pd.DataFrame({"user": users,
                           "user_flair": user_flairs,
                           "title": titles,
                           "post_text": post_texts,
                           "post_date": post_dates,
                           "post_flair": post_flairs,
                           "score": scores,
                           "n_comments": n_comments_list #,
                           # "link": links
                           })
    return output
##### DATA CLEANING #####
# create dfs from csvs
adopted = pd.read_csv("D:\\hw\\adopt-proj\\adopt_posts.csv") # now with comments
adoption_1 = pd.read_csv("D:\\hw\\adopt-proj\\adoption_posts.csv")

# drop link column
adopted.drop(columns=["link"], inplace=True)
adoption_1.drop(columns=["link"], inplace=True)

# change this to datetime instead of str
adopted['post_date'] = pd.to_datetime(adopted['post_date'])
adoption_1['post_date'] = pd.to_datetime(adoption_1['post_date'])
# split r/Adopted into comments and posts
adopted_comms = adopted.loc[adopted["is_comment"] == True]
adopted_comms = adopted_comms.drop(columns=["is_comment"])

adopted_posts = adopted.loc[adopted["is_comment"] == False]
adopted_posts = adopted_posts.drop(columns=["is_comment"])


# split r/Adoption into comments and posts
adoption_1_comms = adoption_1.loc[adoption_1["is_comment"] == True]
adoption_1_comms = adoption_1_comms.drop(columns=["is_comment"])

adoption_1_posts = adoption_1.loc[adoption_1["is_comment"] == False]
adoption_1_posts = adoption_1_posts.drop(columns=["is_comment"])
# add some supplementary data from archived source
adoption_2_posts = open_reddit_json("D:\\hw\\adopt-proj\\Adoption_submissions.json")
adoption_2_comms = open_reddit_json("D:\\hw\\adopt-proj\\Adoption_comments.json")
# change the values in post_date to datetime objects

adoption_2_comms['post_date'] = pd.to_datetime(adoption_2_comms['post_date'].astype(int), unit='s').dt.tz_localize("UTC")
adoption_2_posts['post_date'] = pd.to_datetime(adoption_2_posts['post_date'].astype(int), unit='s').dt.tz_localize("UTC")
# concat data sets
adoption_comms = pd.concat([adoption_1_comms,adoption_2_comms])
adoption_posts = pd.concat([adoption_1_posts,adoption_2_posts])

# change this one weird quirk
adoption_posts.loc[adoption_posts["score"] == "•","score"] = np.nan


# remove posts with no post text, also remove duplicates
adoption_posts = adoption_posts.dropna(subset=["user", "post_text"]).drop_duplicates()
adoption_comms = adoption_comms.dropna(subset=["user", "post_text"]).drop_duplicates()
# also do the same for r/Adopted, but this should do nothing
adopted_posts = adopted_posts.dropna(subset=["user", "post_text"]).drop_duplicates()
adopted_comms = adopted_comms.dropna(subset=["user", "post_text"]).drop_duplicates()
# find posts where they have been removed or deleted
pattern = re.compile(r"^\[*\(*(removed|deleted)")
# exclude such posts from the datasets
adoption_posts = adoption_posts.loc[~adoption_posts["post_text"].astype(str).apply(lambda x: True if pattern.match(x) else False)]
adoption_comms = adoption_comms.loc[~adoption_comms["post_text"].astype(str).apply(lambda x: True if pattern.match(x) else False)]
adopted_posts = adopted_posts.loc[~adopted_posts["post_text"].astype(str).apply(lambda x: True if pattern.match(x) else False)]
adopted_comms = adopted_comms.loc[~adopted_comms["post_text"].astype(str).apply(lambda x: True if pattern.match(x) else False)]
# remove posts that are just empty strings
adoption_posts = adoption_posts[adoption_posts["post_text"] != ""].reset_index(drop=True)
adoption_comms = adoption_comms[adoption_comms["post_text"] != ""].reset_index(drop=True)
# should not really change much for r/Adopted data I scraped myself
adopted_posts = adopted_posts[adopted_posts["post_text"] != ""].reset_index(drop=True)
adopted_comms = adopted_comms[adopted_comms["post_text"] != ""].reset_index(drop=True)
# Find adoptees by user flair tags
pattern = re.compile(r"((?<!of an )(adoptee(?!s)\b|adopted|\bTRA\b|\bKAD\b))|(?<!of a )(adoptee(?!s)\b|adopted(?!kid)|adopted(?!child)|\bTRA\b|\bKAD\b)", re.IGNORECASE)
adoption_posts["is_adoptee"] = adoption_posts["user_flair"].astype(str).apply(lambda x: True if pattern.match(x) else False)
adoption_comms["is_adoptee"] = adoption_comms["user_flair"].astype(str).apply(lambda x: True if pattern.match(x) else False)
adopted_posts["is_adoptee"] = adopted_posts["user_flair"].astype(str).apply(lambda x: True if pattern.match(x) else False)
adopted_comms["is_adoptee"] = adopted_comms["user_flair"].astype(str).apply(lambda x: True if pattern.match(x) else False)
# save as csv
# adoption_posts.to_csv("adoption_posts_df.csv",index=False)
# adoption_comms.to_csv("adoption_comms_df.csv",index=False)
# adopted_posts.to_csv("adopted_posts_df.csv",index=False)
# adopted_comms.to_csv("adopted_comms_df.csv",index=False)