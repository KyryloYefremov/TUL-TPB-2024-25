from pyflink.table import (
    EnvironmentSettings,
    TableEnvironment,
    DataTypes,
    TableDescriptor,
    FormatDescriptor,
    Schema
)
from pyflink.table.expressions import col
from pyflink.table import expressions as expr

def main():
    # init Flink Table environment
    env_settings = EnvironmentSettings.in_batch_mode()
    t_env = TableEnvironment.create(env_settings)

    t_env.get_config().get_configuration().set_string("parallelism.default", "1")

    # paths to data
    ratings_path = "u.data"
    movies_path = "u-mod.item"

    # register <u.data> as a temporary table
    ratings_descriptor = TableDescriptor.for_connector("filesystem") \
        .schema(
            Schema.new_builder()
            .column("user_id", DataTypes.INT())
            .column("movie_id", DataTypes.INT())
            .column("rating", DataTypes.INT())
            .column("timestamp", DataTypes.BIGINT())
            .build()
        ) \
        .option("path", ratings_path) \
        .format(
            FormatDescriptor.for_format("csv")
            .option("field-delimiter", "\t")
            .build()
        ) \
        .build()
    t_env.create_temporary_table("ratings", ratings_descriptor)
    ratings = t_env.from_path("ratings")

    # register <u-mod.items> as a temporary table
    movies_descriptor = TableDescriptor.for_connector("filesystem") \
        .schema(
            Schema.new_builder()
            .column("movie_id", DataTypes.INT())
            .column("title", DataTypes.STRING())
            .build()
        ) \
        .option("path", movies_path) \
        .format(
            FormatDescriptor.for_format("csv")
            .option("field-delimiter", "|")
            .build()
        ) \
        .build()
    t_env.create_temporary_table("movies", movies_descriptor)
    movies = t_env.from_path("movies")

    # filter ratings for only 5-star ratings
    five_star_ratings = ratings.filter(col("rating") == 5)

    # count the number of 5-star ratings per movie
    five_star_counts = five_star_ratings.group_by(col("movie_id")) \
                                        .select(
                                            col("movie_id").alias("five_star_movie_id"),
                                            col("movie_id").count.alias("five_star_count")
                                        )

    # count the total number of ratings per movie
    total_ratings = ratings.group_by(col("movie_id")) \
                           .select(
                               col("movie_id").alias("total_movie_id"),
                               col("movie_id").count.alias("total_rating_count")
                           )

    # join five-star counts with total ratings
    ratings_with_counts = five_star_counts.join(total_ratings) \
                                          .where(five_star_counts.five_star_movie_id == total_ratings.total_movie_id) \
                                          .select(
                                              five_star_counts.five_star_movie_id.alias("movie_id_1"),
                                              five_star_counts.five_star_count,
                                              total_ratings.total_rating_count
                                          )

    # add movie titles by joining with `movies` table
    ratings_with_titles = ratings_with_counts.join(movies) \
                                             .where(ratings_with_counts.movie_id_1 == movies.movie_id) \
                                             .select(
                                                 movies.movie_id,
                                                 movies.title,
                                                 ratings_with_counts.five_star_count,
                                                 ratings_with_counts.total_rating_count,
                                                 (ratings_with_counts.five_star_count.cast(DataTypes.FLOAT())
                                                  / ratings_with_counts.total_rating_count).alias("rating_ratio")
                                             ).order_by(col('rating_ratio').desc).fetch(10)

    ratings_with_titles.execute().print()


if __name__ == "__main__":
    main()
