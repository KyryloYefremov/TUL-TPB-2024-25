import pandas as pd
import re

from collections import Counter

import sys
sys.path.insert(0, "/Users/kirillefremov/development/PycharmProjects/TUL-TPB-2024-25/cv01")

from json_reader import read_json # type: ignore


INPUT_FILENAME = "cv01/idnes-data900.json"


def preprocess(art_list: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(art_list)
    df.comments_num.astype("int")
    df.img_count.astype("int")
    df.date = pd.to_datetime(df['date'], errors='coerce', utc=True)
    return df


def main_task(df: pd.DataFrame):
    # print(df.columns)
    # ['title', 'text', 'img_count', 'categories', 'date', 'comments_num', 'link']

    ### 1. Printing articles number
    articles_num = df.__len__()
    print(f"1. Articles number: {articles_num}")

    ### 2. Printing number of duplicated articles
    print(f"2. Number of duplicated articles: {sum(df.title.duplicated())}")
    df = df.drop_duplicates(subset=['title'])
    print(f"   Number of duplicated articles: {sum(df.title.duplicated())}")
    print(f"   Number of articles: {df.title.size}")

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
    print(articles_per_year.astype(int))

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
        # texts_word_counter.update(words)
        texts_word_num += len(words)

    print(f"10. Total number of words: {texts_word_num}")


def bonus_task(df: pd.DataFrame):
    long_words_counter = Counter()
    covid19_word_counter = Counter()
    word_counter = Counter()
    average_words_length_sum = 0
    words_count = 0
    

    for i, text in enumerate(df.text):
        # 1
        words = re.findall(r'\b\w{6,}+\b', text.lower())
        long_words_counter.update(words)

        # 2
        covid19_count = len(re.findall(r'\bcovid[-]?19\b', text.lower()))
        covid19_word_counter[df.title[i]] = covid19_count

        # 3
        words_count = len(re.findall(r'\b\w+\b', text.lower()))
        word_counter[df.title[i]] = words_count

        # 4
        words_length = [len(word) for word in words]
        try:
            average_words_length_sum += sum(words_length) / len(words_length)
        except ZeroDivisionError:
            pass



    ### 1. Printing 8 most frequent words in articles (min 6 signs length)
    print("1. Articles' 8 most frequent words 6+ letters:")
    for (w, n) in long_words_counter.most_common(8):
        print(f"\t{w} - {n}")

    ### 2. Printing 3 atricles with the highest frequency of the word Covid-19
    print("2. Articles with most frequent appearence of 'Covid-19':")
    for title, num in covid19_word_counter.most_common(3):
        print(f"\t'{title}' = {num}")

    ### 3. Printing articles with maximum and minimum words count
    most = word_counter.most_common()
    max_words_article = most[0]
    min_words_article = most[-1]
    print(f"3. Atricle with max word count: \n\t'{max_words_article[0]}' = {max_words_article[1]}")
    print(f"   Atricle with min word count: \n\t'{min_words_article[0]}' = {min_words_article[1]}")

    ### 4. Printing average word length
    average_wl = round(average_words_length_sum / df.title.size)
    print(f"4. Average word length: {average_wl}")

    ### 5. Printing month with max and min published articles
    articles_per_month = df.groupby(df['date'].dt.month).size().reset_index(name='articles_count')
    max_articles = articles_per_month.max().astype(int)
    min_articles = articles_per_month.min().astype(int)
    print(f"5. Month with max published articles:")
    print(f"\tMonth: {max_articles["date"]} = {max_articles["articles_count"]}")
    print(f"   Month with min published articles:")
    print(f"\tMonth: {min_articles["date"]} = {min_articles["articles_count"]}")


if __name__ == "__main__":
    articles_list = read_json(INPUT_FILENAME)
    df = preprocess(articles_list)
    main_task(df)
    print("=" * 100, "\nBONUS TASKS\n")
    bonus_task(df)