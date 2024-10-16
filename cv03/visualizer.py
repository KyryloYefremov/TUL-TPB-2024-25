import sys
sys.path.append("/Users/kirillefremov/development/PycharmProjects/TUL-TPB-2024-25")

import matplotlib.pyplot as plt
import pandas as pd

from collections import Counter

import cv01.json_reader as json_reader
import cv02.analyzer as analyzer


def main_task(df: pd.DataFrame):
    # print(df.title.size)
    ### 1.

    ### 2. Column graph to plot articles number per year publication
    # articles_per_year = df.groupby(df['date'].dt.year).size().reset_index(name='articles_count')
    # x = articles_per_year['date'].astype(int)
    # y = articles_per_year['articles_count']
    # fig, ax = plt.subplots(figsize=(10, 6))
    # ax.set_title("Articles per years")
    # ax.set_xlabel("Years")
    # ax.set_ylabel("Articles num")
    # ax.bar(x, y, tick_label=x)
    # plt.xticks(rotation=90)

    ### 3. Scatter graph that shows relation btw article length and comments number
    # articles_lens = [len(text.split()) for text in df.text]
    # articles_comments = df.comments_num
    # plt.figure(figsize=(10, 6))
    # plt.scatter(articles_lens, articles_comments, color='blue', alpha=0.7, s=15)
    # plt.title("Article Length and Number of Comments")
    # plt.xlabel("Article len (words num)")
    # plt.ylabel("Num of comments")

    ### 4. Pie graph that shows articles per category
    cats = [cat for cat_list in df.categories for cat in cat_list]
    articles_per_cat_count = Counter(cats)

    # How many top categories you want to show
    top_n = 30

    # Get the most common categories
    top_categories = articles_per_cat_count.most_common(top_n)

    # Separate the top categories and calculate the count for 'Others'
    top_categories_names = [cat[0] for cat in top_categories]
    top_categories_counts = [cat[1] for cat in top_categories]

    # Calculate the 'Others' count
    others_count = sum(count for cat, count in articles_per_cat_count.items() if cat not in top_categories_names)

    # Append the 'Others' category
    top_categories_names.append('Others')
    top_categories_counts.append(others_count)

    # Create the pie chart
    plt.figure(figsize=(8, 8))
    plt.pie(top_categories_counts, labels=top_categories_names, autopct='%1.1f%%', startangle=140)
    plt.title("Articles per Category")

    # Equal aspect ratio ensures that pie chart is a circle
    plt.axis('equal')
    plt.show()




if __name__ == "__main__":
    articles_json = json_reader.read_json(analyzer.INPUT_FILENAME)
    df = analyzer.preprocess(articles_json, drop_duplicates=True)

    main_task(df)