import csv
from conf import config
from classes.Operator import Operator
from Tools.configWriter import ConfigWriter

map_joins = config.map_joins


# Collection of Methods for different purposes


# Obtain the Memory Percentage used of the Cluster
def cluster_memory_index(System_Metrics,taskmanagers=config.taskmanagers):
    memory_available = config.max_heap * config.memory_fraction  # Do not forget to multiply by memory split
    ram_index = 0.0
    cpu_index = 0.0

    for host in taskmanagers:
        memory_used = 0.0

        for metric in System_Metrics.keys():

            if "Status/JVM/Memory/Heap/Used" in metric and host[0] in metric:
                memory_used = memory_used + System_Metrics[metric][0]

        ram_index = ram_index + memory_used / memory_available

    memory_index = (ram_index) / ( len(config.taskmanagers))

    print("Overall cluster Memory Usage: " + str(memory_index*100)+ " %")
    print("")
    return memory_index


# Get the Cluster Overall Load in Percentage (Memory and CPU)
def cluster_health_index(System_Metrics,taskmanagers=config.taskmanagers):
    memory_available = config.max_heap * config.memory_fraction  # Do not forget to multiply by memory split
    ram_index = 0.0
    cpu_index = 0.0

    for host in taskmanagers:
        memory_used = 0.0
        cpu_load = 0.0
        for metric in System_Metrics.keys():

            if "Status/JVM/Memory/Heap/Used" in metric and host[0] in metric:
                memory_used = memory_used + System_Metrics[metric][0]

            if "Status/JVM/CPU/Load" in metric and host[0] in metric:
                cpu_load = cpu_load + System_Metrics[metric][0]
                # Rather do a sum in case we get multiple metrics (due to the pattern we use...)
                # However, only one field should contain the actual value, others are 0.0

        ram_index = ram_index + memory_used / memory_available
        cpu_index = cpu_index + cpu_load
        print("")
        print("CPU-Usage of host " + host[1] + ": " + str(cpu_load*100) + " %")
        print("RAM-Usage of host " + host[1] + ": " + str(memory_used / memory_available*100)+ " %")


    health_index = (ram_index + cpu_index) / (2 * len(config.taskmanagers))

    print("Overall cluster Load: " + str(health_index))
    print("")
    return health_index



# Generating stub Metric/Operator objects. If you want to store more metrics, add here.
def generate_metrics(operators=config.operators,config_file="config.yaml"):

    config = ConfigWriter().readConfig(config_file)
    metrics = {}
    for operator in operators:
        parallelism = 0
        benchmark = config["benchmark"]
        if not "overall" in operator.lower():
            parallelism=int(config[operator])
        else:
            if benchmark.lower() == "nexmark": # Setup Parallelism. We mostly measure Overall latency in the last Operaotr, just not in Query8, there is a special Window operator
                if "query4" in operator.lower(): parallelism = int(config["Query4_AverageByCategory"])
                if "query5" in operator.lower(): parallelism = 1
                if "query6" in operator.lower(): parallelism = int(config["Query6_AverageSellerPrice_WindowFunction"])
                if "query7" in operator.lower(): parallelism = 1
                if "query8" in operator.lower(): parallelism = int(config["parallelism"])

            elif benchmark.lower() == "psm":
                if "query1" in operator.lower(): parallelism = int(config["Sink_1_Parallelism"])
                if "query2" in operator.lower(): parallelism = int(config["Sink_2_Parallelism"])
                if "query3" in operator.lower(): parallelism = int(config["Sink_3_Parallelism"])
                if "query4" in operator.lower(): parallelism = int(config["Sink_4_Parallelism"])
                if "query5" in operator.lower(): parallelism = int(config["Sink_5_Parallelism"])


        if parallelism == 0 or "cogroup" in operator.lower() or operator in map_joins or "join" in operator.lower():
            parallelism = int(config["parallelism"])  # Take overall parallelism

        if "allwindow" in operator.lower():
            parallelism = 1


        operator_obj = Operator(operator, parallelism)

        operator_obj.add_metric("Throughput/m1_rate")

        operator_obj.add_metric("numRecordsInPerSecond/m1_rate")

        if "coprocess" in operator.lower():

            #Latency Metrics - CoProcess
            operator_obj.add_metric("Latency")
            operator_obj.add_metric("Latency_1")
            operator_obj.add_metric("Latency_2")
            operator_obj.add_metric("WaitingTime")
            operator_obj.add_metric("WaitingTime_1")
            operator_obj.add_metric("WaitingTime_2")

        elif "join" in operator.lower() or operator in map_joins:
            # Latency Metrics - Join
            operator_obj.set_parallelism(int(config["parallelism"]))
            operator_obj.add_metric("Latency")
            operator_obj.add_metric("WaitingTime")
            operator_obj.add_metric("WaitingTime_1")
            operator_obj.add_metric("WaitingTime_2")

        elif "cogroup" in operator.lower():

            operator_obj.set_parallelism(int(config["parallelism"]))
            operator_obj.add_metric("Throughput/m1_rate")

            # Latency Metrics - "normal" operators
            operator_obj.add_metric("Latency")
            operator_obj.add_metric("WaitingTime")


        elif "overall" in operator.lower():
            # Throughput metrics - Overall
            operator_obj.add_metric("Throughput/m1_rate")

            # Latency Metrics - Overall
            operator_obj.add_metric("Latency")

        elif "input" in operator.lower():
            # Input metrics - input Rate
            operator_obj.add_metric("m1_rate")


        else:
            # Throughput metrics - "normal" operators
            operator_obj.add_metric("Throughput/m1_rate")

            # Latency Metrics - "normal" operators
            operator_obj.add_metric("Latency")
            operator_obj.add_metric("WaitingTime")


        # Window Metrics
        operator_obj.add_metric("WindowSize/m1_rate")

        # Operator Parallelism
        operator_obj.add_metric("Parallelism")


        # Buffer metrics
        operator_obj.add_metric("inputQueueLength")
        operator_obj.add_metric("outputQueueLength")

        metrics[operator] = operator_obj

    return metrics


# DEPRECATED
def read_in_metrics_csv(filename):
    infile = open(filename, mode='r')
    reader = csv.reader(infile)
    k = [rows[1] for rows in reader]

    infile = open(filename, mode='r')
    reader = csv.reader(infile)
    parallelisms = [int(rows[2]) for rows in reader]


    # v list is stub for data and parallelism of operators.
    # 0.0 : Later Metric value, 0: value for rank, par: Query parallelism
    v = [[0.0,0, par] for par in parallelisms]


    return dict(zip(k, v))

# Still used for System Metrics
def read_in_system_metrics_csv(filename, taskmanagers, jobmanager, job_name):
    infile = open(filename, mode='r')
    reader = csv.reader(infile)
    k = []
    for rows in reader:
        for hosts in taskmanagers:
            k.append("/" + hosts[1] + "/taskmanager/" + hosts[0] + "/" + rows[1])
            k.append("/" + jobmanager + "/" + rows[1])
            k.append("/ibm-power-1/stateSize/" + rows[1] )

            k.append("/" + hosts[1] + "/network/" + rows[1])
        k.append("/" + jobmanager + "/" + job_name + "/" + rows[1])


    v = [[0.0, 0] for i in range(len(k))]

    return dict(zip(k, v))



# DEPRECATED, using Metric and Operator Objects now. So no need here
def join_nexmark_throughput(Metrics):
    Metrics["Query4_Price_per_closed_Auction_CoProcessFunction-Throughput"][0] = \
        Metrics["Query4_Price_per_closed_Auction_CoProcessFunction-Throughput_1/m1_rate"][0] + \
        Metrics["Query4_Price_per_closed_Auction_CoProcessFunction-Throughput_2/m1_rate"][0]
    Metrics.pop("Query4_Price_per_closed_Auction_CoProcessFunction-Throughput_1/m1_rate")
    Metrics.pop("Query4_Price_per_closed_Auction_CoProcessFunction-Throughput_2/m1_rate")

    Metrics["Query4_Auctions_Items_CoProcessFunction-Throughput"][0] = \
        Metrics["Query4_Auctions_Items_CoProcessFunction-Throughput_2/m1_rate"][0] + \
        Metrics["Query4_Auctions_Items_CoProcessFunction-Throughput_1/m1_rate"][0]
    Metrics.pop("Query4_Auctions_Items_CoProcessFunction-Throughput_2/m1_rate")
    Metrics.pop("Query4_Auctions_Items_CoProcessFunction-Throughput_1/m1_rate")

    Metrics["Query5_Auction_to_Item_CoProcessFunction-Throughput"][0] = \
        Metrics["Query5_Auction_to_Item_CoProcessFunction-Throughput_2/m1_rate"][0] + \
        Metrics["Query5_Auction_to_Item_CoProcessFunction-Throughput_1/m1_rate"][0]
    Metrics.pop("Query5_Auction_to_Item_CoProcessFunction-Throughput_2/m1_rate")
    Metrics.pop("Query5_Auction_to_Item_CoProcessFunction-Throughput_1/m1_rate")

    Metrics["Query6_Auctions_to_Bids_CoProcessFunction-Throughput"][0] = \
        Metrics["Query6_Auctions_to_Bids_CoProcessFunction-Throughput_2/m1_rate"][0] + \
        Metrics["Query6_Auctions_to_Bids_CoProcessFunction-Throughput_1/m1_rate"][0]
    Metrics.pop("Query6_Auctions_to_Bids_CoProcessFunction-Throughput_2/m1_rate")
    Metrics.pop("Query6_Auctions_to_Bids_CoProcessFunction-Throughput_1/m1_rate")

    Metrics["Query7_ActiveAuctions_Items_CoProcessFunction-Throughput"][0] = \
        Metrics["Query7_ActiveAuctions_Items_CoProcessFunction-Throughput_2/m1_rate"][0] + \
        Metrics["Query7_ActiveAuctions_Items_CoProcessFunction-Throughput_1/m1_rate"][0]
    Metrics.pop("Query7_ActiveAuctions_Items_CoProcessFunction-Throughput_2/m1_rate")
    Metrics.pop("Query7_ActiveAuctions_Items_CoProcessFunction-Throughput_1/m1_rate")

    Metrics["Query5_Auction_to_Item_CoProcessFunction-Latency"][0] = \
        (Metrics["Query5_Auction_to_Item_CoProcessFunction-Latency_1"][0] + \
        Metrics["Query5_Auction_to_Item_CoProcessFunction-Latency_2"][0]) / 2
    Metrics.pop("Query5_Auction_to_Item_CoProcessFunction-Latency_1")
    Metrics.pop("Query5_Auction_to_Item_CoProcessFunction-Latency_2")

    Metrics["Query5_Auction_to_Item_CoProcessFunction-WaitingTime"][0] = \
        (Metrics["Query5_Auction_to_Item_CoProcessFunction-WaitingTime_1"][0] + \
         Metrics["Query5_Auction_to_Item_CoProcessFunction-WaitingTime_2"][0]) / 2
    Metrics.pop("Query5_Auction_to_Item_CoProcessFunction-WaitingTime_1")
    Metrics.pop("Query5_Auction_to_Item_CoProcessFunction-WaitingTime_2")

    Metrics["Query6_Auctions_to_Bids_CoProcessFunction-WaitingTime"][0] = \
        (Metrics["Query6_Auctions_to_Bids_CoProcessFunction-WaitingTime_2"][0] + \
         Metrics["Query6_Auctions_to_Bids_CoProcessFunction-WaitingTime_1"][0]) / 2
    Metrics.pop("Query6_Auctions_to_Bids_CoProcessFunction-WaitingTime_2")
    Metrics.pop("Query6_Auctions_to_Bids_CoProcessFunction-WaitingTime_1")

    Metrics["Query6_Auctions_to_Bids_CoProcessFunction-Latency"][0] = \
        (Metrics["Query6_Auctions_to_Bids_CoProcessFunction-Latency_2"][0] + \
        Metrics["Query6_Auctions_to_Bids_CoProcessFunction-Latency_1"][0]) / 2
    Metrics.pop("Query6_Auctions_to_Bids_CoProcessFunction-Latency_2")
    Metrics.pop("Query6_Auctions_to_Bids_CoProcessFunction-Latency_1")

    Metrics["Query4_Auctions_Items_CoProcessFunction-Latency"][0] = \
        (Metrics["Query4_Auctions_Items_CoProcessFunction-Latency_1"][0] + \
        Metrics["Query4_Auctions_Items_CoProcessFunction-Latency_2"][0]) / 2
    Metrics.pop("Query4_Auctions_Items_CoProcessFunction-Latency_1")
    Metrics.pop("Query4_Auctions_Items_CoProcessFunction-Latency_2")

    Metrics["Query4_Auctions_Items_CoProcessFunction-WaitingTime"][0] = \
        (Metrics["Query4_Auctions_Items_CoProcessFunction-WaitingTime_1"][0] + \
        Metrics["Query4_Auctions_Items_CoProcessFunction-WaitingTime_2"][0]) / 2
    Metrics.pop("Query4_Auctions_Items_CoProcessFunction-WaitingTime_1")
    Metrics.pop("Query4_Auctions_Items_CoProcessFunction-WaitingTime_2")

    Metrics["Query4_Price_per_closed_Auction_CoProcessFunction-Latency"][0] = \
        (Metrics["Query4_Price_per_closed_Auction_CoProcessFunction-Latency_1"][0] + \
        Metrics["Query4_Price_per_closed_Auction_CoProcessFunction-Latency_2"][0]) / 2
    Metrics.pop("Query4_Price_per_closed_Auction_CoProcessFunction-Latency_1")
    Metrics.pop("Query4_Price_per_closed_Auction_CoProcessFunction-Latency_2")

    Metrics["Query4_Price_per_closed_Auction_CoProcessFunction-WaitingTime"][0] = \
        (Metrics["Query4_Price_per_closed_Auction_CoProcessFunction-WaitingTime_1"][0] + \
        Metrics["Query4_Price_per_closed_Auction_CoProcessFunction-WaitingTime_2"][0]) / 2
    Metrics.pop("Query4_Price_per_closed_Auction_CoProcessFunction-WaitingTime_1")
    Metrics.pop("Query4_Price_per_closed_Auction_CoProcessFunction-WaitingTime_2")

    Metrics["Query7_ActiveAuctions_Items_CoProcessFunction-Latency"][0] = \
        (Metrics["Query7_ActiveAuctions_Items_CoProcessFunction-Latency_1"][0] + \
        Metrics["Query7_ActiveAuctions_Items_CoProcessFunction-Latency_2"][0]) / 2
    Metrics.pop("Query7_ActiveAuctions_Items_CoProcessFunction-Latency_1")
    Metrics.pop("Query7_ActiveAuctions_Items_CoProcessFunction-Latency_2")

    Metrics["Query7_ActiveAuctions_Items_CoProcessFunction-WaitingTime"][0] = \
        (Metrics["Query7_ActiveAuctions_Items_CoProcessFunction-WaitingTime_1"][0] + \
        Metrics["Query7_ActiveAuctions_Items_CoProcessFunction-WaitingTime_2"][0]) / 2
    Metrics.pop("Query7_ActiveAuctions_Items_CoProcessFunction-WaitingTime_1")
    Metrics.pop("Query7_ActiveAuctions_Items_CoProcessFunction-WaitingTime_2")

    Metrics["Query8_Person_Auction_JoinFunction-WaitingTime"][0] = \
        (Metrics["Query8_Person_Auction_JoinFunction-WaitingTime_2"][0] + \
        Metrics["Query8_Person_Auction_JoinFunction-WaitingTime_1"][0]) / 2
    Metrics.pop("Query8_Person_Auction_JoinFunction-WaitingTime_2")
    Metrics.pop("Query8_Person_Auction_JoinFunction-WaitingTime_1")
