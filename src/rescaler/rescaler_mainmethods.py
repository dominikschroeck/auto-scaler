from Tools.configWriter import ConfigWriter
from Tools.metricsReader import MetricsReader
from conf import config
from restarter.client import send_conf

from Tools.util import generate_metrics
from Tools.util import read_in_system_metrics_csv

#############
## Methods ##
#############

# This file basically contains some methods the rescaler uses. Makes the main "rescaler.py" smaller and improves readability

def get_Metrics():
    # config_file = sys.argv[1]
    maxParallelism = config.maxParallelism
    job_name = config.job_name
    taskmanagers = config.taskmanagers
    jobmanager = config.jobmanager + "/jobmanager"

    Metrics = generate_metrics()


    System_Metrics = read_in_system_metrics_csv(config.filename_system_metrics, taskmanagers, jobmanager, job_name)

    start_path = config.start_path

    node_paths = [start_path + jobmanager]
    node_paths.append(start_path + "ibm-power-1/")

    for taskmanager in taskmanagers:
        node_paths.append(start_path + taskmanager[1] + "/taskmanager/" + taskmanager[0])


    # Read Metrics
    reader = MetricsReader()

    for path in node_paths:

        reader.read_metrics_whisper_obj(Metrics, path, taskmanagers)

        reader.read_metrics_whisper(System_Metrics, path, taskmanagers, 6)

    for operator in Metrics.keys():
        Metrics[operator].clean_metrics()
        # Identifying the maximum-ever throughput under current parallelism
        metric_object=Metrics[operator].get_metric_object("Throughput/m1_rate")
        lis = metric_object.get_list()
        max_throughput_now = 0
        if (len(lis)>0):
            max_throughput_now = max(lis)

        if max_throughput_now > metric_object.get_max():
            metric_object.set_max(max_throughput_now)


    for key, value in list(System_Metrics.items()):
        if value[0] == 0.0: del System_Metrics[key]

    return Metrics, System_Metrics

def Scale(decision,Metrics,job_config="config.yaml",out="out.yaml"):
    if decision != None:
        print("")
        print("****************************************")
        print("*** Scale - Sending Decision to Flink***")
        print("****************************************")
        print("")

        # Send modified config file to Server and let it do the work
        # Decision: (highest_latency_operator, optimal_parallelism, True)

        wr = ConfigWriter()


        #wr.writeConfig(Metrics, job_config, out)

        overall_para = 0
        scaled_operators = decision[0]

        for operator in scaled_operators.keys():
            if operator in Metrics.keys(): # If an operator has been scaled up
                if scaled_operators[operator] > 128:
                    Metrics[operator].set_parallelism(128)
                else:
                    Metrics[operator].set_parallelism(scaled_operators[operator])
            if operator == "parallelism":
                overall_para = scaled_operators[operator] # If the overall parallelism has been increased (Join, CoGroup...)


        if decision[1] == True:
            wr.writeConfig(Metrics, job_config, out, decision="out",overall_para=overall_para)

            send_conf(filename=out)

        else:
            wr.writeConfig(Metrics, job_config, out, decision="up",overall_para=overall_para)
            send_conf(filename=out)







