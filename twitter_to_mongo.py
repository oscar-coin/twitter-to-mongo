import pymongo
import argparse
import mongo


def main():
    args = parse_args()
    db = mongo.get_mongo_database_with_auth(args.dbhost, args.dbport, args.dbname, args.username, args.password)
    print(db["imdb_data"])


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dbhost', help='Address of MongoDB server', default="127.0.0.1")
    parser.add_argument('--dbport', help='Port of MongoDB server', default=27017)
    parser.add_argument('--dbname', '-n', help='Database name', type=str, required=True)
    parser.add_argument('--username', help='Database user', default=None)
    parser.add_argument('--password', help='Password for the user', default=None)
    parser.add_argument('--collection', '-v', help='Collection name for Twitter archive data',
                        default="twitter_archive")

    args = parser.parse_args()

    return args


if __name__ == '__main__':
    main()
