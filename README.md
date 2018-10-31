# Building an Optimizer for Dynamic Scaling of Stream Processing Engine

This repo contains the code generated for the Master Thesis of Dominik Schroeck, at TU Berlin. It was supervised by Jeyhun Karimov and Bonaventura Del Monte (DFKI).
The thesis deals with developing an optimizer that decides dynamically if scaling out of certain operators out of the streaming topology makes sense.

This benchmark is used to learn about Stream processing, distributed computing and to support the theoretical model. The code comes with two very different benchmarks supposed to allow evaluation
of streaming performance. You can use the code to identify bottlenecks and test your own scaling algorithms on it. The benchmarks make use of checkpointing/fault tolerance and many stateful operators
to test the impact of checkpointing mechanisms as well.

We employ [Apache Flink](https://flink.apache.org) as Streaming Framework as it is state-of-the art, open-source and gives eactly-once guarantees.

## Components

### Kafka Producer
We use a kafka producer that computes data and stores them into Apache Kafka for consuming by Apache Flink.

Location: [src/kafka-producers](https://gitlab.tu-berlin.de/dominikschroeck/master-thesis/tree/master/src/kafka-producers)

You can configure the producer in the producer_conf.yaml Yaml file! 
```bash
java -jar run producer.jar /path/to/config_file
# Add Memory by using -Xmx10000m to add a lot of memory! The more the better. After a while, you will run into trouble otherwise
```

### Benchmark Libraries
Stores some shared classes that are required by both, the producer and the benchmarks. Just run the "build_and_install" sh/Powershell scripts. This will automatically compile and install the package into your local mvn repo.

Location: [src/benchmark_libraries](https://gitlab.tu-berlin.de/dominikschroeck/master-thesis/tree/master/src/benchmark_libraries)

Here you will find the custom serializers for improving throughput.

### Benchmarks
The Project "benchmark_largestate" contains the Java Project for the two Benchmarks (PSM and NexMark).

Location: [src/benchmark_largestate](https://gitlab.tu-berlin.de/dominikschroeck/master-thesis/tree/master/src/benchmark_largestate)
You run the jobs using the standard Flink way. 
```bash
flink run benchmark_largestate.jar /path/to/config_file
```

### Rescaler
The actual rescaler is available in [src/rescaler](https://gitlab.tu-berlin.de/dominikschroeck/master-thesis/tree/master/src/rescaler)
Note that it is highly tailored for the benchmarks and requires a lot of reconfiguration to match other benchmarks. It employs many Custom Metrics you first need to employ! Plus naming for the operators. The configuration file config.py gives great hints.

Requires [src/scripts/stateSize_measure.py](https://gitlab.tu-berlin.de/dominikschroeck/master-thesis/tree/master/src/scripts7stateSize_measure.py) running on the Job Master node in background. This script invokes a DU -h on a HDFS folder of your choice to measure the state size. If you do not use HDFS, change the command the script emits.

The whole rescaler is tailored to work on cluster of TU Berlin and my personal server. If you have the time, make it more general. Should not be too complex. Simple Python.

The approach is easy to udnerstand:
- After 3 Latency Violations of one query in a row: Take An action
- Take top $k$ slowest (highest latency) operators and compute optimal parallelism to tackle input rate:
 - For Windows: Add 2 Channels --> Bring a better idea, please! I was fighting with different custom metrics but no solution to identify optimal parallelism!
 - For Continuous operators: Optimal Parallelism = Input Rate / Throughput Per channel
 
After computing the optimal parallelism, the algorithm checks whether the cluster runs under very high load and if scaling out (Adding one compute node) is required. 

## Compile Benchmarks and Start them
Easiest way is to enter [src/scripts](https://gitlab.tu-berlin.de/dominikschroeck/master-thesis/tree/master/src/scripts) and run the (Linux) script build_package.sh. It will compile all relevant parts, install them to your MVN repo and output into the subdirectory "build-target".

### Configuration files
You can use configuration files that are available in [experiments](https://gitlab.tu-berlin.de/dominikschroeck/master-thesis/tree/master/experiments). The producer also requires config files which are available there as well.

### Run the Rescaler
```bash
python3 rescaler.py
```
Run this on the node that runs Graphite and configure the [src/rescaler/conf/config.py](https://gitlab.tu-berlin.de/dominikschroeck/master-thesis/tree/master/src/rescaler/conf/config.py) file to read form the whisper directory. Most importantly, configure the taskmanagers in the config.py! On Scaling it copies a new configuration file to a web server directory. Please amend this in the code for your needs!

The jobmanager node requires the [server.py](https://gitlab.tu-berlin.de/dominikschroeck/master-thesis/tree/master/src/rescaler/restarter/server.py) to run. It regularly checks the webserver for the new config file and downloads. Why this complex setup, and not direct TCP/IP Connection? The cluster I use is behind a firewall and restricts direct TCP IP connections! Soon, I will change this!
The script will restart the job creating a savepoint and use the updated configuration file.

## Requirements
- JDK 1.8 or higher
- Maven
- Graphite set up using Whisper (default setup) [https://graphiteapp.org/](https://graphiteapp.org/)
- Apache Kafka (Scripts for creating topics available in [src/scripts](https://gitlab.tu-berlin.de/dominikschroeck/master-thesis/tree/master/src/scripts) )
- Linux (although also should work in windows for local deployment) 

*Please note that this whole project is so tailored for my master thesis jobs and the cluster environment that you will have to change a lot of setup (using my personal server you don't have access to, Cluster node names...).*
