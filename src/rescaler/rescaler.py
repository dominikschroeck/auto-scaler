#######################################
##      MAGIC RESCALER FOR FLINK    ###
#######################################

# This script starts the whole "Magic" of rescaling an Apache Flink Job by monitoring Latency and if one Latency Metric
# surpassed the accepted latencyfor the last 3 monitoring periods

from conf import config
import time
import rescaler_mainmethods
from costFunction.costFunction import cost
from Tools.util import cluster_health_index
import os

latency_dict_overall = {}
congested_operators = []
operator_inputrates = {}
all_operators = []
take_action = False
#print("Ignoring Window Operators in this code!!")
while (True):

    interval = config.metric_interval * 60
    # Get System Metrics
    Metrics, System_Metrics = rescaler_mainmethods.get_Metrics()

    # Analyze the Latency metrics of each operator
    # Get from Congestion Finder method, but just latency.
    # I guess yesterday = now - 20s (of m1 rate), most closest --> DONE

    # Overall Analysis - Latency of queries
    latency_dict = {}
    for key in Metrics.keys():
            if "overall" in key.lower():
                latency = Metrics[key].get_latency()
                print("Latency of Operator: " + key + ": " + str(latency) )
                if latency >= config.accepted_latency:  # Found a latency violation in a query
                    latency_dict[key] = [1,latency]

    # Now clean up Overall Latency dict whenever we do not have the same key in the dict

    old_keyset = latency_dict_overall.keys()
    new_keyset = latency_dict.keys()

    # 1 Wenn key_new in key_old : +1
    # 2 Wenn key_new nicht in key_old: füge hinzu
    # 3 Wenn key_old nicht in key_new: -1 , remove if <=0



    for key_new in new_keyset:  # Add in case we already have the key in the overall set
        if key_new in old_keyset:
            if latency_dict_overall[key_new][1] < latency_dict[key_new][1]: # Only if latency increased sicne the last iteration
                latency_dict_overall[key_new] = [latency_dict_overall[key_new][0] + 1,latency_dict[key_new][1]]
            else:
                latency_dict_overall[key_new] = [latency_dict_overall[key_new][0]-1,latency_dict[key_new][1]]
        else: # Add key
            latency_dict_overall[key_new] = [1,latency_dict[key_new][1]]


    # Check for the key if it was not part of the congested queries last epoch.
    for k, v in list(latency_dict_overall.items()):
        if k not in new_keyset:
            if latency_dict_overall[k][0] <= 0: # Delete because 0 anyway, but we should never reach here, right?
                del latency_dict_overall[k]
            else: # Decrease by 1
                latency_dict_overall[k] = [latency_dict_overall[k][0] - 1, latency_dict_overall[k][1]]


    # Iterate over list and find the ones with a 3, i.e. 3 latency violations
    for key in latency_dict_overall.copy():
        if latency_dict_overall[key][0] >= 3:
            take_action = True

        if latency_dict_overall[key][0] <= 0:
            del latency_dict_overall[key]

    print()
    print("List of Congested Queries: ")
    print(latency_dict_overall)
    print()

    cluster_health_index(System_Metrics)

    if take_action:
        latency_dict_overall = {} # CLear the monito
        # Find Congested Operator(s)

        CostFunction = cost()
        decision = CostFunction.evaluateCost(Metrics,System_Metrics,Operators=config.operators)
        # Decide if to scale out or up
        #scaled_operators, scaleOut = rescaler_mainmethods.EvaluateCost(Metrics, System_Metrics, congested_operators,
                                                                       #operator_inputrates, all_operators)
        #if scaled_operators is not None and scaleOut is not None:
        rescaler_mainmethods.Scale(decision,Metrics)

        # Rather wait and extend the next time we check for latency and related problems. First, the
        # job has to work off the backpressure
        interval = interval * 6 # Add an approach for finding the new ID. Problem: We cannot reach the host externally -.-
        # So I will use the new config
        os.system("cp out.yaml config.yaml") # Use as new config file to base decisions on

    print("Iteration done, next one in " + str(interval) + " seconds!")
    time.sleep(interval)  # Interval to check metrics
    print("----------------------------------------------------------------")


