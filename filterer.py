import sys

__author__ = 'emre'

import pymongo
from pymongo.collection import Collection
from datetime import datetime
from collections import defaultdict

date_format = "%Y-%m-%d"
date_format_year_only = "%Y"

current_data_timestamp = datetime(2015, 11, 1)
next_year_timestamp = datetime(2016, 1, 1)


def filter_movie(movie_json):
    """
    Checks if data related to given movie should be kept according to the following criteria.

    Hard filters first.

    Release date:
    1. Remove movies with missing release date. They are so far out that there are no release dates, or the data is
    not very reliable.
    2. Remove movies scheduled for release later than 2015.

    Languages:
    1. Remove languages that do not contain a languages field.
    2. Remove movies that do not contain English.

    Runtime:
    1. Remove movies with unknown runtime.
    2. Remove movies with runtime less than 40 minutes, as per eligibility requirements.

    Rating:
    1. Remove movies that were released earlier than 14 days before dataset timestamp and do not have a rating
    and a rating count.
    2. Remove movies with rating less than 6.0.
    3. Remove movies with rating count less than 1000, released earlier than 14 days before dataset timestamp.

    Countries:
    1. Remove movies with no country data.
    2. Remove movies that do not originate from USA.

    :param movie_json: movie element coming from MongoDB
    """
    # Retrieve release date
    release_date = None

    if "releaseDate" in movie_json:
        try:
            release_date = datetime.strptime(movie_json["releaseDate"], date_format)
        except ValueError:
            # Somehow the date format does not match, let's check if it is year only
            try:
                release_date = datetime.strptime(movie_json["releaseDate"], date_format_year_only)
            except ValueError:
                pass  # We did everything we could.
    else:
        print("Warning: Movie release date (field: releaseDate) not found for film id {0}!".format(movie_json["_id"]))

    # Retrieve languages
    languages = None
    if "languages" in movie_json:
        languages = movie_json["languages"]

    # Retrieve runtime
    runtime = None

    if "runtime" in movie_json:
        runtime = int(movie_json["runtime"].split()[0])
    else:
        pass
        # print("Warning: Film length (field: runtime) not found for film id {0}!".format(movie_json["_id"]))

    # Retrieve country data
    countries = None
    if "countries" in movie_json:
        countries = movie_json["countries"]

    # Retrieve rating data
    avg_score = None
    rating_count = None

    if "rating" in movie_json:
        if "avgScore" in movie_json["rating"]:
            avg_score = float(movie_json["rating"]["avgScore"])

        if "ratingCount" in movie_json["rating"]:
            rating_count = int(movie_json["rating"]["ratingCount"].replace(",", ""))
    else:
        # Possible structural error?
        print("Warning: Rating (field: rating) not found for film id {0}!".format(movie_json["_id"]))

    # print(runtime)
    # print(avg_score)
    # print(rating_count)
    # print(movie_json)
    # Debugging
    if runtime is not None and runtime < 40:
        print("runtime < 40 for id {0}".format(movie_json["_id"]))

    # Filters
    if release_date is None:
        return "no_release_date"

    if (release_date - next_year_timestamp).days >= 0:
        return "released_after_new_year"

    if languages is None:
        return "no_languages"

    if "english" not in [e.lower() for e in languages]:
        return "not_english"

    if runtime is None:
        return "no_runtime"

    if runtime < 40:
        return "too_short"

    if countries is None:
        return "no_countries"

    if "usa" not in [e.lower() for e in countries]:
        return "not_usa"

    if avg_score is None and rating_count is None and (current_data_timestamp - release_date).days >= 14:
        return "missing_rating_for_released"

    if avg_score is not None and avg_score < 6.0:
        return "rating_too_low"

    if rating_count is not None and rating_count < 1000:
        return "not_rated_enough"

    return "ok"


def fetch_filters(imdb_movie_collection: Collection):
    i = 0
    movie_names = set()
    movie_urls = set()
    writer_names = set()
    director_names = set()
    actor_names = set()
    character_names = set()

    num_movies = imdb_movie_collection.count()

    for movie in imdb_movie_collection.find():
        if i % 20000 == 0:
            print("Processing {0} out of {1} movies...".format(i, num_movies))

        result = filter_movie(movie)

        if result == "ok":
            try:
                movie_urls.add(movie["url"])
                movie_names.add(movie["title"])
                if "writers" in movie: writer_names.update(set(movie["writers"]))
                if "director" in movie: director_names.add(movie["director"])

                actors = [actor for actor in movie["castMembers"] if
                          "type" in actor and actor["type"] in ["Actor", "Actress"]]

                actor_names.update(set(actor["actorName"] for actor in actors))
                character_names.update(set(actor["characterName"] for actor in actors if actor["characterName"] != ""))

                actor_names.update(actor_names)
                character_names.update(character_names)

            except TypeError as err:
                print("Error occurred during processing for movie with the following details.\nId: {0}\nURL:{1}".format(
                    movie["_id"], movie["url"]))
                print("Error: {0}".format(err))

        i += 1

        # if i == 100: break

    with open("keywords.txt", "w") as f:
        print("\n".join(movie_names), file=f)
        print("\n".join(movie_urls), file=f)
        print("\n".join(writer_names), file=f)
        print("\n".join(director_names), file=f)
        print("\n".join(actor_names), file=f)
        print("\n".join(character_names), file=f)
