

import os
import sys
#from pyspark import StorageLevel, SparkFiles
from pyspark.sql import SparkSession, DataFrame, SQLContext

if __name__ == "__main__":

    outName=sys.argv[1]
    numTrials = int(sys.argv[2])

    #import findspark
    #findspark.init('/Users/jerdavis/devlib/spark-2.4.3-bin-hadoop2.7/')
    #findspark.init()

    #os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /Users/jerdavis/devlib/xgboost/xgboost4j-spark-0.90.jar,/Users/jerdavis/devlib/xgboost/xgboost4j-0.90.jar,/Users/jerdavis/devhome/projects/pysparkgw/target/scala-2.11/pysparkgw_2.11-0.1.jar pyspark-shell'
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars gs://jdtrader/bin/coinproc-1.0-SNAPSHOT.jar,gs://jdtrader/bin/coinproc-1.0-SNAPSHOT-jar-with-dependencies.jar,gs://spark-lib/bigquery/spark-bigquery-latest.jar pyspark-shell'
    #findspark.init()

    #/mnt/var/log/hadoop/steps


    from pyspark.sql.session import SparkSession

    # .master("local[*]")\
    spark = SparkSession.builder.appName("foo")\
        .config("spark.driver.memory","8G")\
        .config("spark.driver.maxResultSize", "2G") \
        .getOrCreate()
        # .config("spark.driver.extraClassPath", "s3://bitdatawest/pbin/xgboost4j-spark-0.90.jar:s3://bitdatawest/pbin/xgboost4j-0.90.jar:s3://bitdatawest/pbin/pysparkgw_2.11-0.1.jar")\
        # .config("spark.executor.extraClassPath", "s3://bitdatawest/pbin/xgboost4j-spark-0.90.jar:s3://bitdatawest/pbin/xgboost4j-0.90.jar:s3://bitdatawest/pbin/pysparkgw_2.11-0.1.jar")\
        # .config("spark.jars", "s3://bitdatawest/pbin/xgboost4j-spark-0.90.jar:s3://bitdatawest/pbin/xgboost4j-0.90.jar:s3://bitdatawest/pbin/pysparkgw_2.11-0.1.jar")\
        #.getOrCreate()

    sqlContext = spark._wrapped

    barWrapper = spark.sparkContext._jvm.jd.dataproc.apps.PyBridge

    print('############PRE##############')
    barWrapper.test()

    trainDf = DataFrame(barWrapper.prepDf(spark.sparkContext._jsc,'jdtrader.work.hyper_train',256),sqlContext)
    testDf = DataFrame( barWrapper.prepDf(spark.sparkContext._jsc,'jdtrader.work.hyper_test',256), sqlContext)


    def train_help(args):
        try:
            barWrapper = spark.sparkContext._jvm.jd.dataproc.apps.PyBridge

            best_score = barWrapper.eval(spark.sparkContext._jsc,
                                        'gs://jdtrader/gen/hyper/',
                                         trainDf._jdf,
                                         testDf._jdf,
                                         int(args['params']['num_folds']),
                                         int(args['params']['max_bins']),
                                         int(args['params']['max_depth']),
                                         int(args['params']['num_trees']))

            return 1.0 - best_score
        except Exception as e:
            print(str(e))
            return float(1)


    import pickle
    from hyperopt import tpe, hp, fmin, Trials

    param_space = hp.choice('foo', [{'model': 'foom', 'params': {'num_folds': hp.quniform('num_folds', 1, 3, 1),
                                                                 'max_bins': hp.quniform('max_bins', 32, 1024, 32),
                                                                 'max_depth': hp.quniform('max_depth', 8, 256, 8),
                                                                 'num_trees': hp.quniform('num_trees', 8, 256, 8)
                                                                 }}])


    def run_trials():
        trials_step = 1  # how many additional trials to do after loading saved trials. 1 = save after iteration
        max_trials = 2  # initial max_trials. put something small to not have to wait

        try:  # try to load an already saved trials object, and increase the max
            trials = pickle.load(open("/tmp/"+outName+".hyperopt", "rb"))
            print("Found saved Trials! Loading...")
            max_trials = len(trials.trials) + trials_step
            print("Rerunning from {} trials to {} (+{}) trials".format(len(trials.trials), max_trials, trials_step))
        except:  # create a new trials object and start searching
            trials = Trials()

        # best = fmin(fn=mymodel, space=model_space, algo=tpe.suggest, max_evals=max_trials, trials=trials)
        best_params = fmin(fn=train_help, space=param_space, algo=tpe.suggest, max_evals=max_trials, trials=trials)

        print("Best:", best_params)

        # save the trials object
        with open("/tmp/"+outName+".hyperopt", "wb") as f:
            pickle.dump(trials, f)


    for x in range(numTrials):
        run_trials()
