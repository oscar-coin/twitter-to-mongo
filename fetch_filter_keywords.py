import traceback
from collections import defaultdict

__author__ = 'emre'

from datetime import datetime
from enum import Enum

from pymongo.collection import Collection

date_format = "%d %B %Y"
date_format_year_only = "%Y"

current_data_timestamp = datetime(2015, 11, 1)
next_year_timestamp = datetime(2016, 1, 1)


class Status(Enum):
    OK = 0
    NO_RELEASE_DATE = 1
    RELEASED_AFTER_NEW_YEAR = 2
    NO_LANGUAGES = 3
    NOT_ENGLISH = 4
    NO_RUNTIME = 5
    TOO_SHORT = 6
    MISSING_RATING_FOR_RELEASED = 7
    RATING_TOO_LOW = 8
    NOT_RATED_ENOUGH = 9
    NO_COUNTRIES = 10
    NOT_USA = 11


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

    if "releaseInfo" in movie_json:
        for country_release_date in movie_json["releaseInfo"]:
            try:
                if country_release_date["Country"] == "USA":
                    release_date = datetime.strptime(country_release_date["Date"], date_format)
            except ValueError:
                # Somehow the date format does not match, let's check if it is year only
                try:
                    release_date = datetime.strptime(country_release_date["Date"], date_format_year_only)
                except ValueError:
                    pass  # We did everything we could.
    else:
        print("Warning: Movie release date (field: releaseInfo) not found for film id {0}!".format(movie_json["_id"]))

    # Retrieve languages
    languages = None
    if "languages" in movie_json:
        languages = movie_json["languages"]

    # Retrieve runtime
    runtime = None

    if "runtime" in movie_json:
        try:
            runtime = int(movie_json["runtime"].replace(",", "").split()[0])
        except:
            print("Error while parsing runtime for movie with id {}!".format(movie_json["_id"]))
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

    # Filters
    if release_date is None:
        return Status.NO_RELEASE_DATE

    if (release_date - next_year_timestamp).days >= 0:
        return Status.RELEASED_AFTER_NEW_YEAR

    if languages is None:
        return Status.NO_LANGUAGES

    if "english" not in [e.lower() for e in languages]:
        return Status.NOT_ENGLISH

    if runtime is None:
        return Status.NO_RUNTIME

    if runtime < 40:
        return Status.TOO_SHORT

    if avg_score is None and rating_count is None and (current_data_timestamp - release_date).days >= 14:
        return Status.MISSING_RATING_FOR_RELEASED

    if avg_score is not None and avg_score < 6.0:
        return Status.RATING_TOO_LOW

    if rating_count is not None and rating_count < 1000 and (current_data_timestamp - release_date).days >= 14:
        return Status.NOT_RATED_ENOUGH

    if countries is None:
        return Status.NO_COUNTRIES

    if "usa" not in [e.lower() for e in countries]:
        return Status.NOT_USA

    return Status.OK


def fetch_filtering_keywords(imdb_movie_collection: Collection):
    i = 0
    movie_names = set()
    movie_urls = set()
    writer_names = set()
    director_names = set()
    actor_names = set()
    character_names = set()

    stats = defaultdict(lambda: 0)

    num_movies = imdb_movie_collection.count()
    print("Found {} movies in total.".format(num_movies))

    for movie in imdb_movie_collection.find():
        if i % 20000 == 0:
            print("Processing {0} out of {1} movies...".format(i, num_movies))

        result = filter_movie(movie)
        stats[result] += 1

        if result == Status.OK:
            try:
                movie_urls.add(movie["url"])
                movie_names.add(movie["title"])

                if "writers" in movie:
                    writer_names.update(set(e["name"] for e in movie["writers"]))

                if "director" in movie:
                    director_names.add(movie["director"]["name"])

                if "castMembers" in movie:
                    actor_names.update(set(actor["name"]
                                           for actor in movie["castMembers"]
                                           if "name" in actor and
                                           actor["name"] != ""))
                    character_names.update(set(actor["characterName"]
                                               for actor in movie["castMembers"]
                                               if "characterName" in actor and
                                               actor["characterName"] != ""))

                    actor_names.update(actor_names)
                    character_names.update(character_names)

            except:
                print("Error occurred during processing for movie with the following details.\nId: {0}\nURL:{1}".format(
                    movie["_id"], movie["url"]))
                tb = traceback.format_exc()
                print(tb)

        i += 1

    print("Number of unique movies names: {}".format(len(movie_names)))
    print("Number of unique movie URLs: {}".format(len(movie_urls)))
    print("Number of unique writer names: {}".format(len(writer_names)))
    print("Number of unique writer names: {}".format(len(director_names)))
    print("Number of unique actor names: {}".format(len(actor_names)))
    print("Number of unique character names: {}".format(len(character_names)))

    print("Statistics:")
    for k, v in stats.items():
        print("{}: {}".format(k, v))

    with open("movie_names.txt", "w") as f:
        print("\n".join(movie_names), file=f)

    with open("movie_urls.txt", "w") as f:
        print("\n".join(movie_urls), file=f)

    with open("writer_names.txt", "w") as f:
        print("\n".join(writer_names), file=f)

    with open("director_names.txt", "w") as f:
        print("\n".join(director_names), file=f)

    with open("actor_names.txt", "w") as f:
        print("\n".join(actor_names), file=f)

    with open("character_names.txt", "w") as f:
        print("\n".join(character_names), file=f)
