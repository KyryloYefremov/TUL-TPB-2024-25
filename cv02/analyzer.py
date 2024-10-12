import pandas as pd
import pprint as pp

import sys
sys.path.insert(0, "/Users/kirillefremov/development/PycharmProjects/TUL-TPB-2024-25/cv01")

from json_reader import read_json # type: ignore


INPUT_FILENAME = "cv01/idnes-data250.json"


def preprocess(art_list: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(art_list)
    df.comments_num.astype("int")
    df.img_count.astype("int")
    return df


def main():
    articles_list = read_json(INPUT_FILENAME)
    df = preprocess(articles_list)

    # print(df.columns)
    # ['title', 'text', 'img_count', 'categories', 'date', 'comments_num', 'link']

    ### Printing articles number
    articles_num = df.__len__()
    print(f"1. Articles number: {articles_num}")

    ### Printing number of duplicated articles
    duplicates_num = sum(df.title.duplicated())
    print(f"2. Number of duplicated articles: {duplicates_num}")

    ### Printing the date of the oldest article
    oldest_date = df.date.min()
    print(f"3. Atricle's oldest date: {oldest_date}")

    ### Printing an article name with the most comments number
    max_comments_num = df.max()["comments_num"]
    max_comments_article = df._get_value(df.idxmax()["comments_num"], "title")
    print(f"4. Article name: '{max_comments_article}'.\n\tComments num: {max_comments_num}")

    ### Printing the highest number of added photos of an article
    max_article_photos = df.img_count.max()
    print(f"5. Highest number of added photos: {max_article_photos}")

    




if __name__ == "__main__":
    main()