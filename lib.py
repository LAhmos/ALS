import os
from pyspark.mllib.recommendation import ALS

import logging
import math

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_counts_and_averages(ID_and_ratings_tuple):
    """Given a tuple (movieID, ratings_iterable)
    returns (movieID, (ratings_count, ratings_avg))
    """
    nratings = len(ID_and_ratings_tuple[1])
    return ID_and_ratings_tuple[0], (nratings, float(sum(x for x in ID_and_ratings_tuple[1])) / nratings)


class RecommendationEngine:
    """A movie recommendation engine
    """

    def __count_and_average_ratings(self):
        """Updates the movies ratings counts from
        the current data self.ratings_RDD
        """
        logger.info("Counting movie ratings...")
        movie_ID_with_ratings_RDD = self.ratings_RDD.map(lambda x: (x[1], x[2])).groupByKey()
        movie_ID_with_avg_ratings_RDD = movie_ID_with_ratings_RDD.map(get_counts_and_averages)
        self.movies_rating_counts_RDD = movie_ID_with_avg_ratings_RDD.map(lambda x: (x[0], x[1][0]))

    def __train_model(self):
        """Train the ALS model with the current dataset
        """
        logger.info("Training the ALS model...")

        self.model = ALS.train(self.ratings_RDD, self.rank, seed=self.seed,
                               iterations=self.itera, lambda_=self.lamda)
        logger.info("ALS model built!")


    def setRecPar(self,irank, iiter, ilama):
        self.rank=irank
        self.itera=iiter
        self.lamda=ilama

    def eval_error_with_Kfold(self,  iK):
        """Train the ALS model with the current dataset
        """

        errors = []

        print("traing model lamda:{0}   rank:{1}    iter:{2}    k:{3}".format(self.lamda, self.rank, self.itera, iK))
        for i in range(0, iK):
            training, valdiation = self.trainToTrainEvaWithK(self.train, iK, i)
            valdiationForPred = valdiation.map(lambda x: (x[0], x[1]))

            model = ALS.train(training, self.rank, seed=self.seed,iterations=self.itera, lambda_=self.lamda)

            # test validation
            valiPred = model.predictAll(valdiationForPred).map(lambda r: ((r[0], r[1]), r[2]))
            ratesAndPredForEva = valdiation.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(valiPred)
            error = math.sqrt(ratesAndPredForEva.map(lambda r: (r[1][0] - r[1][1]) ** 2).mean())

            errors.append( error)

        print("Kfold done!")
        return sum(errors) / iK

    def setTrainTest(self):
        self.train, self.test = self.ratings_RDD.randomSplit([.75, .25], 0)

    def getTestforPred(self):
        return self.test.map(lambda x: (x[0], x[1]))

    def getTrainforPred(self):
        return self.train.map(lambda x: (x[0], x[1]))

    def trainOnTrainingSetWithPar(self, iRank, iIter, iLamda):
        self.model = ALS.train(self.train, iRank, seed=self.seed, iterations=iIter, lambda_=iLamda)
    def trainOnTrainingSet(self):
        self.model = ALS.train(self.train, self.rank, seed=self.seed, iterations=self.itera, lambda_=self.lamda)

    def getTestError(self):
        testPred = self.model.predictAll(self.getTestforPred()).map(lambda r: ((r[0], r[1]), r[2]))
        ratesAndPredFortest = self.test.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(testPred)
        testError = math.sqrt(ratesAndPredFortest.map(lambda r: (r[1][0] - r[1][1]) ** 2).mean())
        return testError

    def getTrainError(self):
        trainPred = self.model.predictAll(self.getTrainforPred()).map(lambda r: ((r[0], r[1]), r[2]))
        ratesAndPredForTrain = self.train.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(trainPred)
        trainError = math.sqrt(ratesAndPredForTrain.map(lambda r: (r[1][0] - r[1][1]) ** 2).mean())
        return trainError

    def __predict_ratings(self, user_and_movie_RDD):
        """Gets predictions for a given (userID, movieID) formatted RDD
        Returns: an RDD with format (movieTitle, movieRating, numRatings)
        """
        predicted_RDD = self.model.predictAll(user_and_movie_RDD)
        predicted_rating_RDD = predicted_RDD.map(lambda x: (x.product, x.rating))
        predicted_rating_title_and_count_RDD = \
            predicted_rating_RDD.join(self.movies_titles_RDD).join(self.movies_rating_counts_RDD)
        predicted_rating_title_and_count_RDD = \
            predicted_rating_title_and_count_RDD.map(lambda r: (r[1][0][1], r[1][0][0], r[1][1]))

        return predicted_rating_title_and_count_RDD

    def predict_all(self):
        predict_train = self.model.predictAll(self.ratings_RDD).map(lambda r: ())
        return

    def trainToTrainEvaWithK(self, set, k, index):
        partSize = set.count() / k
        # print("size {}\n",set.count())
        # print("part {}\n", set.count()/k)
        # print("index {} index end {}\n", index*partSize ,index*partSize +partSize)
        eval = set.zipWithIndex().filter(lambda key: index * partSize <= key[1] < index * partSize + partSize).map(
            lambda r: r[0])
        train = set.zipWithIndex().filter(lambda key: not index * partSize <= key[1] < index * partSize + partSize).map(
            lambda r: r[0])
        return train, eval

    # def add_ratings(self, ratings):
    #     """Add additional movie ratings in the format (user_id, movie_id, rating)
    #     """
    #     # Convert ratings to an RDD
    #     new_ratings_RDD = self.sc.parallelize(ratings)
    #     # Add new ratings to the existing ones
    #     self.ratings_RDD = self.ratings_RDD.union(new_ratings_RDD)
    #     # Re-compute movie ratings count
    #     self.__count_and_average_ratings()
    #     # Re-train the ALS model with the new ratings
    #     self.__train_model()
    #
    #     return ratings

    # def get_ratings_for_movie_ids(self, user_id, movie_ids):
    #     """Given a user_id and a list of movie_ids, predict ratings for them
    #     """
    #     requested_movies_RDD = self.sc.parallelize(movie_ids).map(lambda x: (user_id, x))
    #     # Get predicted ratings
    #     ratings = self.__predict_ratings(requested_movies_RDD).collect()
    #
    #     return ratings

    def get_top_ratings(self, user_id, movies_count):
        """Recommends up to movies_count top unrated movies to user_id
        """
        # Get pairs of (userID, movieID) for user_id unrated movies
        user_unrated_movies_RDD = self.ratings_RDD.filter(lambda rating: not rating[0] == user_id) \
            .map(lambda x: (user_id, x[1])).distinct()
        # Get predicted ratings
        ratings = self.__predict_ratings(user_unrated_movies_RDD).filter(lambda r: r[2] >= 25).takeOrdered(movies_count,
                                                                                                           key=lambda
                                                                                                               x: -x[1])

        return ratings

    def __init__(self, sc, dataset_path):
        """Init the recommendation engine given a Spark context and a dataset path
        """

        logger.info("Starting up the Recommendation Engine: ")

        self.sc = sc

        # Load ratings data for later use
        logger.info("Loading Ratings data...")
        ratings_file_path = os.path.join(dataset_path, 'ratings.csv')
        ratings_raw_RDD = self.sc.textFile(ratings_file_path)
        ratings_raw_data_header = ratings_raw_RDD.take(1)[0]
        self.ratings_RDD = ratings_raw_RDD.filter(lambda line: line != ratings_raw_data_header) \
            .map(lambda line: line.split(",")).map(
            lambda tokens: (int(tokens[0]), int(tokens[1]), float(tokens[2]))).cache()

        # Load movies data for later use
        logger.info("Loading Movies data...")
        movies_file_path = os.path.join(dataset_path, 'movies.csv')
        movies_raw_RDD = self.sc.textFile(movies_file_path)

        movies_raw_data_header = movies_raw_RDD.take(1)[0]
        self.movies_RDD = movies_raw_RDD.filter(lambda line: line != movies_raw_data_header) \
            .map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]), tokens[1], tokens[2])).cache()

        self.movies_titles_RDD = self.movies_RDD.map(lambda x: (int(x[0]), x[1])).cache()
        # Pre-calculate movies ratings counts
        self.__count_and_average_ratings()
        self.model=0

       # Train the model
        self.rank = 8
        self.seed = 5
        self.itera = 10
        self.lamda = 0.1

        # # self.__train_model()
        #
        # self.__train_model_with_Kfold(4, 5, .1, 5)
