from pymongo import MongoClient

__author__ = 'emre'


def get_mongo_database_with_auth(dbhost, dbport, dbname, username, password):
    """
    Attempts to get authenticated access to a MongoDB database.

    :param dbname: database name for which a client will be obtained and authenticated
    :return: :raise "Failed to authenticate to MongoDB database {0} using given username and password!".format: when
    authentication or connection fails
    """
    client = MongoClient(dbhost, dbport)

    db = client[dbname]

    if username is not None or password is not None:
        if not db.authenticate(username, password):
            raise "Failed to authenticate to MongoDB database {0} using given username and password!".format(dbname)

    return db
