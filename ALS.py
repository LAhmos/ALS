import lib
from pyspark import SparkContext
def sweepRank():
    return als

sc = SparkContext("local", "Simple App")
als=lib.RecommendationEngine(sc,"/home/ahmad/Desktop/ALS/data")
als.setTrainTest()
als.setRecPar(2,2,1.0)
als.trainOnTrainingSet()
print(als.getTrainError())
print(als.eval_error_with_Kfold(5))
print(als.getTestError())

