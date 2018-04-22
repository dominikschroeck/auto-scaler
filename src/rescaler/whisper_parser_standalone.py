from Tools.metricsReader import MetricsReader
from Tools.util import read_in_system_metrics_csv
import time
from Tools.util import generate_metrics

# print ('Number of arguments:', len(sys.argv), 'arguments.')
# print ('Argument List:', str(sys.argv))
# read config
# exit()
# [operator-name [latency,throughput]
from conf import config

# config_file = sys.argv[1]
maxParallelism = config.maxParallelism
job_name = config.job_name
taskmanagers = config.taskmanagers
jobmanager = config.jobmanager + "/jobmanager"

start = time.time()


    #job_id = JobLoader().getJobs("http://" + config.jobmanager + ":" +config.jobmanager_port)


#Metrics = read_in_metrics_csv(config.filename_operator_metrics)
Metrics = generate_metrics()

System_Metrics = read_in_system_metrics_csv(config.filename_system_metrics, taskmanagers, jobmanager, job_name)

operators = config.operators


accepted_latency = config.accepted_latency

start_path = config.start_path

node_paths = [start_path + jobmanager]
node_paths += [start_path + "ibm-power-1/stateSize"]


#job_id = JobLoader().getJobs("http://" + config.jobmanager + ":" +config.jobmanager_port)
job_id = "42352"
if job_id is None: exit()


for taskmanager in taskmanagers:
    node_paths.append(start_path + taskmanager[1] + "/taskmanager/" + taskmanager[0])
    #node_paths.append(start_path + taskmanager[1] + "/network")

# Read Metrics
reader = MetricsReader()
Metrics = generate_metrics()

now=int(time.time())

print(node_paths)
for path in node_paths:

    reader.read_metrics_whisper_obj_to_csv(Metrics,path,now=now)
    print("Reading System Metrics!")
    reader.read_metrics_whisper_to_csv(System_Metrics, path, taskmanagers,now=now)


print("RUNTIME:")
print(time.time() - start)