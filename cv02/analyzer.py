import pandas as pd
import pprint as pp
import re

from collections import Counter

import sys
sys.path.insert(0, "/Users/kirillefremov/development/PycharmProjects/TUL-TPB-2024-25/cv01")

from json_reader import read_json # type: ignore


INPUT_FILENAME = "cv01/idnes-data250.json"


def preprocess(art_list: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(art_list)
    df.comments_num.astype("int")
    df.img_count.astype("int")
    df.date = pd.to_datetime(df['date'], errors='coerce', utc=True)
    return df


def main():
    articles_list = read_json(INPUT_FILENAME)
    df = preprocess(articles_list)

    # print(df.columns)
    # ['title', 'text', 'img_count', 'categories', 'date', 'comments_num', 'link']

    ### 1. Printing articles number
    articles_num = df.__len__()
    print(f"1. Articles number: {articles_num}")

    ### 2. Printing number of duplicated articles
    duplicates_num = sum(df.title.duplicated())
    print(f"2. Number of duplicated articles: {duplicates_num}")

    ### 3. Printing the date of the oldest article
    oldest_date = df.date.min()
    print(f"3. Atricle's oldest date: {oldest_date}")

    ### 4. Printing an article name with the most comments number
    max_comments_num = df.max()["comments_num"]
    max_comments_article = df._get_value(df.idxmax()["comments_num"], "title")
    print(f"4. Article name: '{max_comments_article}'.\n\tComments num: {max_comments_num}")

    ### 5. Printing the highest number of added photos of an article
    max_article_photos = df.img_count.max()
    print(f"5. Highest number of added photos: {max_article_photos}")

    ### 6. Printing the number of articles per date of publishing
    articles_per_year = df.groupby(df['date'].dt.year).size().reset_index(name='articles_count')
    print("6. Articles per year:")
    print(articles_per_year)

    ### 7. Printing the number of unique cats and the number of articles in every cat
    cats = [cat for cat_list in df.categories for cat in cat_list]
    articles_per_cat_count = Counter(cats)
    unique_cats = len(articles_per_cat_count)
    print(f"7. Unique categories: {unique_cats}")
    print("Articles per category:")
    for i, cat in enumerate(articles_per_cat_count.keys()):
        print(f"\t'{cat}' - {articles_per_cat_count[cat]}")
        if i >= 5: break

    ### 8. Printing 5 most frequent words from articles' titles since 2021
    titles_word_counter = Counter()
    articles_after_2021 = df[df["date"].dt.year > 2020.0]
    for title in articles_after_2021.title:
        words = re.findall(r'\b\w+\b', title.lower())
        titles_word_counter.update(words)

    print("8. Titles' 5 most frequent words:")
    for (w, n) in titles_word_counter.most_common(5):
        print(f"\t{w} - {n}")

    ### 9. Printing total comments number
    total_comments = df.comments_num.sum()
    print(f"9. Total number of comments: {total_comments}")

    ### 10. Printing total number of words in all articles
    texts_word_num = 0
    texts_word_counter = Counter()
    for text in df.text:
        words = re.findall(r'\b\w+\b', text.lower())
        texts_word_counter.update(words)
        texts_word_num += len(words)

    print(f"10. Total number of words: {texts_word_num}, {texts_word_counter.__len__()}")


    




    





if __name__ == "__main__":
    main()