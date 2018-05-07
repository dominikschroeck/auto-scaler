maxParallelism = 128
benchmark = "nexmark"
job_name = "nexmark_5min_incremental_scale_allqueries_24032018"
taskmanagers = [
    ("e9a868f867970813764ca4e81ed6ed67", "ibm-power-2"),
    ("c49b5476261193ef1c55583ff06f6092", "ibm-power-4"),
    #("30db0d5fe3bbf61efb9673ea2879350f", "ibm-power-8")
]
jobmanager = "ibm-power-1"  # /jobmanager"
jobmanager_port = "8391"
path_to_jar = "/home/flink/benchmark.jar"  # path on server to Jar file
path_to_flink = "/home/flink/flink-Metrics"  # Path to the Flink main folder on jobmanager
metric_interval = 2  # Data interval to check during each epoch in minutes
number_to_scale = 3
savepoint_path = "hdfs://ibm-power-1.dima.tu-berlin.de:44000/user/dschroeck/save"

max_downtime = 3 * 60  # Accepted Downtime in seconds

accepted_latency = 1000.0
# Flink Settings for TaskManager. I need to know this! Or maybe later take the Max from Flinks Metrics? ;)
max_heap = 18 * 1024 * 1024 * 1024  # 8 GB Heap
memory_fraction = 0.766  # Fraction of memory that is available

filename_system_metrics = "system-metrics.csv"

# Names of Operators. They have to be part of the metric name!
operators = [
    # Operators for NexMark:
    "Query5_Auction_to_Item_CoProcessFunction",
    "Query5_DetermineHottestItem_AllWindowFunction",
    "Query5-Overall",
    "Query6_Auctions_to_Bids_CoProcessFunction",
    "Query6_AverageSellerPrice_WindowFunction",
    "Query6-Overall",
    "Query4_Auctions_Items_CoProcessFunction",
    "Query4_Price_per_closed_Auction_CoProcessFunction",
    "Query4_AverageByCategory",
    "Query4-Overall",
    "Query7_ActiveAuctions_Items_CoProcessFunction",
    "Query7_AllWindowFunction",
    "Query7-Overall",
    "Query8-Overall",
    # "Query8_Person_Auction_JoinFunction",
    "TriggerWindow(TumblingEventTimeWindows(10000)",
    # Operators for PSM:
    "Query1_WindowFunction",
    "Query2_RichWindowFunction",
    "Query3_RichWindowFunction",
    "Query4_RichCoGroupFunction",
    # "Query4_RichWindowFunction",
    "Query5_RichCoGroupFunction",
    "Query3-Overall", "Query2-Overall", "Query1-Overall"
]

map_joins = ["TriggerWindow(TumblingEventTimeWindows(10000)"]

# start_path = "/home/dominik/whisper/"  # Storage of Whisper
start_path = "/opt/graphite/storage/whisper/"

max_network = 125000000  # 1 Gbit in byte
