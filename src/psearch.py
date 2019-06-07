
#aws emr create-cluster --applications Name=Hadoop Name=Ganglia Name=Zeppelin Name=Spark --bootstrap-actions Path=s3://bitdatawest/bin/bootstrap.sh --ec2-attributes '{"KeyName":"aws1","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-8d1c01e5","EmrManagedSlaveSecurityGroup":"sg-9a71edfc","EmrManagedMasterSecurityGroup":"sg-9f71edf9"}' --service-role EMR_DefaultRole --enable-debugging --release-label emr-5.23.0 --log-uri 's3://aws-logs-467768410616-us-west-2/elasticmapreduce/'  --name 'bm' --instance-groups '[{"InstanceCount":3,"BidPrice":"1.0","InstanceGroupType":"CORE","InstanceType":"r4.8xlarge","Name":"Core instance group - 2"},{"InstanceCount":1,"BidPrice":"1.0","InstanceGroupType":"MASTER","InstanceType":"r3.4xlarge","Name":"Master instance group - 1"}]' --configurations '[{"Classification":"spark","Properties":{"maximizeResourceAllocation":"true"},"Configurations":[]}]' --region us-west-2


#aws emr add-steps --cluster-id j-1BCCJULWS4JGJ --steps Type=Spark,Name=spec,Args=[--jars,"s3://bitdatawest/pbin/xgboost4j-spark-0.90.jar:s3://bitdatawest/pbin/xgboost4j-0.90.jar:s3://bitdatawest/pbin/pysparkgw_2.11-0.1.jar",--conf,spark.memory.fraction=0.95,--conf,spark.memory.storageFraction=0.05,--conf,spark.executor.extraJavaOptions="-Xss16m -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseParallelGC -XX:GCTimeRatio=99",--conf,spark.executor.memoryOverhead=20000,--conf,spark.executor.memory=200g,--conf,spark.executor.cores=20,--conf,spark.executor.instances=12,--conf,spark.dynamicAllocation.enabled=false,--deploy-mode,client,--master,yarn,--driver-memory,6g,s3://bitdatawest/pbin/psearch.py],ActionOnFailure=CONTINUE

#aws emr add-steps --cluster-id j-1BCCJULWS4JGJ --steps Type=Spark,Name=spec,Args=[--conf,spark.memory.fraction=0.95,--conf,spark.memory.storageFraction=0.05,--conf,spark.executor.extraJavaOptions="-Xss16m -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseParallelGC -XX:GCTimeRatio=99",--conf,spark.executor.memoryOverhead=20000,--conf,spark.executor.memory=200g,--conf,spark.executor.cores=30,--conf,spark.executor.instances=12,--conf,spark.dynamicAllocation.enabled=false,--deploy-mode,client,--master,yarn,--driver-memory,6g,s3://bitdatawest/pbin/psearch.py],ActionOnFailure=CONTINUE

#aws emr add-steps --cluster-id j-1BCCJULWS4JGJ --jars s3://bitdatawest/pbin/xgboost4j-spark-0.90.jar,s3://bitdatawest/pbin/xgboost4j-0.90.jar,s3://bitdatawest/pbin/pysparkgw_2.11-0.1.jar --steps Type=Spark,Name=spec,Args=[--conf,spark.memory.fraction=0.95,--conf,spark.memory.storageFraction=0.05,--conf,spark.executor.extraJavaOptions="-Xss16m -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseParallelGC -XX:GCTimeRatio=99",--conf,spark.executor.memoryOverhead=20000,--conf,spark.executor.memory=200g,--conf,spark.executor.cores=20,--conf,spark.executor.instances=12,--conf,spark.dynamicAllocation.enabled=false,--deploy-mode,client,--master,yarn,--driver-memory,6g,s3://bitdatawest/pbin/psearch.py],ActionOnFailure=CONTINUE

#aws emr add-steps --cluster-id j-1BCCJULWS4JGJ --steps file:///Users/jerdavis/devhome/projects/paramsearch/src/step.json

#spark-submit --jars s3://bitdatawest/pbin/xgboost4j-spark-0.90.jar,s3://bitdatawest/pbin/xgboost4j-0.90.jar,s3://bitdatawest/pbin/pysparkgw_2.11-0.1.jar --conf spark.memory.fraction=0.95 --conf spark.memory.storageFraction=0.05 --conf spark.executor.memoryOverhead=20000 --conf spark.executor.memory=200g --conf spark.executor.cores=20 --conf spark.executor.instances=12 --conf spark.dynamicAllocation.enabled=false --deploy-mode client --master yarn --driver-memory 6g s3://bitdatawest/pbin/psearch.py


import os

if __name__ == "__main__":


    #import findspark
    #findspark.init('/Users/jerdavis/devlib/spark-2.4.3-bin-hadoop2.7/')
    #findspark.init()

    #os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /Users/jerdavis/devlib/xgboost/xgboost4j-spark-0.90.jar,/Users/jerdavis/devlib/xgboost/xgboost4j-0.90.jar,/Users/jerdavis/devhome/projects/pysparkgw/target/scala-2.11/pysparkgw_2.11-0.1.jar pyspark-shell'
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars s3://bitdatawest/pbin/xgboost4j-spark-0.90.jar,s3://bitdatawest/pbin/xgboost4j-0.90.jar,s3://bitdatawest/pbin/pysparkgw_2.11-0.1.jar pyspark-shell'
    #findspark.init()

    #/mnt/var/log/hadoop/steps


    from pyspark.sql.session import SparkSession

    # .master("local[*]")\
    spark = SparkSession.builder.appName("foo")\
        .config("spark.driver.memory","8G")\
        .config("spark.driver.maxResultSize", "2G")\
        .config("spark.driver.extraClassPath", "s3://bitdatawest/pbin/xgboost4j-spark-0.90.jar:s3://bitdatawest/pbin/xgboost4j-0.90.jar:s3://bitdatawest/pbin/pysparkgw_2.11-0.1.jar")\
        .config("spark.executor.extraClassPath", "s3://bitdatawest/pbin/xgboost4j-spark-0.90.jar:s3://bitdatawest/pbin/xgboost4j-0.90.jar:s3://bitdatawest/pbin/pysparkgw_2.11-0.1.jar")\
        .config("spark.jars", "s3://bitdatawest/pbin/xgboost4j-spark-0.90.jar:s3://bitdatawest/pbin/xgboost4j-0.90.jar:s3://bitdatawest/pbin/pysparkgw_2.11-0.1.jar")\
        .getOrCreate()

    xgbWrapper = spark.sparkContext._jvm.jd.PyTest

    print('############PRE##############')
    xgbWrapper.test()


    features = ['ptr_1440o1',
                'ptr_360o3',
                'ptr_360o8',
                'bolbw_60_20',
                'ptr_360o5',
                'ptr_1440o4',
                'ptr_360o6',
                'ptr_1440o10',
                'ptr_360o11',
                'ptr_15o16',
                'ptr_360o4',
                'ptr_1440o9',
                'ptr_360o13',
                'ptr_1440o2',
                'ptr_360o10',
                'ptr_360o12',
                'ptr_15o4',
                'bolbw_1440_20',
                'ptr_1440o6',
                'ptr_360o16',
                'ptr_360o14',
                'ptr_360o18',
                'ptr_60o10',
                'ptr_360o15',
                'ptr_360o7',
                'ptr_15o12',
                'ptr_15o15',
                'ptr_1440o7',
                'ptr_15o7',
                'ptr_15o13',
                'ptr_15o8',
                'ptr_60o1',
                'ptr_15o3',
                'ptr_15o14',
                'ptr_360o9',
                'ptr_1440o5',
                'ptr_15o19',
                'ptr_15o11',
                'ptr_1440o12',
                'ptr_15o18',
                'ptr_360o17',
                'ptr_60o5',
                'trix_60_9',
                'bolbw_5_31',
                'bolbw_15_31',
                'ptr_1440o11',
                'ptr_15o17',
                'trix_60_21',
                'ptr_360o19',
                'ptr_60o6',
                'ptr_15o9',
                'ptr_1440o20',
                'ptr_15o10',
                'ptr_1440o15',
                'ptr_15o6',
                'bolbw_360_20',
                'ptr_1440o14',
                'ptr_15o20',
                'ptr_5o4',
                'ptr_60o15',
                'ptr_60o11',
                'ptr_5o2',
                'bolbw_1_31',
                'ptr_1440o19',
                'ptr_1440o17',
                'ptr_60o9',
                'ptr_360o20',
                'ptr_5o1',
                'trix_360_15',
                'ptr_1440o8',
                'ptr_60o8',
                'ptr_5o17',
                'ptr_5o12',
                'ptr_5o10',
                'ptr_60o14',
                'ptr_1440o18',
                'trix_15_15',
                'ptr_5o5',
                'ptr_60o17',
                'ptr_1440o13',
                'ptr_60o18',
                'ptr_60o16',
                'ptr_5o6',
                'trix_1440_21',
                'ptr_5o7',
                'bolind_1440_31',
                'ptr_60o13',
                'ptr_60o7',
                'ptr_1440o16',
                'trix_1440_9',
                'ptr_60o12',
                'ptr_60o19',
                'ptr_5o15',
                'bpow_1440o11',
                'bpow_1440o9',
                'bpow_1440o12',
                'ptr_5o8',
                'bpow_1440o2',
                'ptr_5o9',
                'ptr_5o18',
                'bpow_1440o17',
                'bpow_360o1',
                'bpow_1440o19',
                'bpow_1440o16',
                'ptr_60o20',
                'bpow_1440o3',
                'bpow_1440o20',
                'bpow_1440o15',
                'bpow_1440o5',
                'bpow_1440o4',
                'bolind_360_31',
                'ptr_5o13',
                'bpow_1440o18',
                'vbolbw_360_20',
                'ptr_1o10',
                'bpow_1440o10',
                'bpow_1440o7',
                'ptr_5o19',
                'ptr_1o5',
                'bpow_1440o8',
                'bpow_1440o14',
                'bpow_1440o6',
                'bpow_1440o1',
                'bpow_360o3',
                'bpow_360o2',
                'bpow_1440o13',
                'vpbol_360_20',
                'bpow_360o4',
                'ptr_1o20',
                'vbolind_1440_31',
                'bpow_360o17',
                'ptr_1o8',
                'ptr_1o1',
                'ptr_1o9',
                'ptr_1o0',
                'bpow_360o10',
                'bpow_360o16',
                'ptr_1o2',
                'ptr_1o7',
                'bpow_360o15',
                'bpow_360o14',
                'ptr_1o14',
                'bpow_360o18',
                'bpow_360o13',
                'bpow_360o12',
                'ptr_1o12',
                'ptr_1o4',
                'trix_5_9',
                'bpow_360o5',
                'bpow_360o7',
                'bpow_360o19',
                'bpow_360o11',
                'ptr_1o13',
                'ptr_1o11',
                'bpow_360o6',
                'ptr_1o3',
                'bpow_360o20',
                'vbolind_360_31',
                'bpow_360o9',
                'bpow_360o8',
                'ptr_1o24',
                'ptr_1o26',
                'ptr_1o16',
                'ptr_1o17',
                'ptr_1o6',
                'ptr_1o15',
                'ptr_1o25',
                'ptr_1o18',
                'ptr_1o21',
                'vbolbw_1_31',
                'ptr_1o22',
                'ptr_1o30',
                'ptr_1o23',
                'ptr_1o27',
                'bpow_60o16',
                'bpow_60o9',
                'bpow_60o8',
                'bpow_60o2',
                'bpow_60o1',
                'bpow_60o7',
                'bpow_60o10',
                'bpow_60o6',
                'vbolbw_5_31',
                'bpow_60o12',
                'bpow_60o17',
                'bpow_60o5',
                'ptr_1o29',
                'bpow_60o4',
                'vpbol_60_20',
                'bpow_60o14',
                'bpow_15o15',
                'bpow_60o3',
                'bpow_5o11',
                'bpow_60o19',
                'bpow_60o15',
                'bpow_60o20',
                'ptr_1o28',
                'bpow_15o14',
                'bpow_15o7']

    response = 'u_300_2600_response'

    def train_help(args):
        try:
            xgbWrapper = spark.sparkContext._jvm.jd.PyTest

            best_score = xgbWrapper.evalFoo(spark.sparkContext._jsc,
                                         's3://dataproc-data/data/gen/gen1_1/',
                                         features,
                                         response,
                                         float(args['params']['eta']),
                                         float(args['params']['gamma']),
                                         int(args['params']['max_depth']),
                                         int(args['params']['max_leaves']),
                                         float(args['params']['min_child_weight']),
                                         float(args['params']['subsample']),
                                         float(args['params']['colsample_bytree']),
                                         float(args['params']['colsample_bylevel']),
                                         float(args['params']['colsample_bynode']),
                                         float(args['params']['lambda']),
                                         float(args['params']['alpha']),
                                         int(args['params']['max_bin']),
                                         int(args['params']['num_parallel_tree']),
                                         int(args['params']['num_round']),
                                         int(args['params']['num_early_stopping_rounds']))

            return 1.0 - best_score
        except Exception as e:
            print(str(e))
            return float(1)


    import pickle
    from hyperopt import tpe, hp, fmin, Trials

    param_space = hp.choice('foo', [{'model': 'foom', 'params': {'eta': hp.uniform('eta', 0, 1),
                                                                 'gamma': 1.0,
                                                                 'max_depth': hp.quniform('max_depth', 3, 60, 3),
                                                                 'max_leaves': 0,
                                                                 'min_child_weight': 1.0,
                                                                 'subsample': 1.0,
                                                                 'colsample_bytree': 1.0,
                                                                 'colsample_bylevel': 1.0,
                                                                 'colsample_bynode': 1.0,
                                                                 'lambda': 1.0,
                                                                 'alpha': 0.0,
                                                                 'max_bin': hp.quniform('max_bin', 32, 512, 32),
                                                                 'num_parallel_tree': hp.quniform('num_parallel_tree', 2,
                                                                                                  60, 2),
                                                                 'num_round': 100,
                                                                 'num_early_stopping_rounds': 10,
                                                                 }}])


    def run_trials():
        trials_step = 1  # how many additional trials to do after loading saved trials. 1 = save after iteration
        max_trials = 2  # initial max_trials. put something small to not have to wait

        try:  # try to load an already saved trials object, and increase the max
            trials = pickle.load(open("/tmp/my_model.hyperopt", "rb"))
            print("Found saved Trials! Loading...")
            max_trials = len(trials.trials) + trials_step
            print("Rerunning from {} trials to {} (+{}) trials".format(len(trials.trials), max_trials, trials_step))
        except:  # create a new trials object and start searching
            trials = Trials()

        # best = fmin(fn=mymodel, space=model_space, algo=tpe.suggest, max_evals=max_trials, trials=trials)
        best_params = fmin(fn=train_help, space=param_space, algo=tpe.suggest, max_evals=max_trials, trials=trials)

        print("Best:", best_params)

        # save the trials object
        with open("/tmp/my_model.hyperopt", "wb") as f:
            pickle.dump(trials, f)


    # loop indefinitely and stop whenever you like
    while True:
        run_trials()
