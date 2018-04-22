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

### Benchmark Libraries
Stores some shared classes that are required by both, the producer and the benchmarks. Just run the "build_and_install" sh/Powershell scripts. This will automatically compile and install the package into your local mvn repo.

Location: [src/benchmark_libraries](https://gitlab.tu-berlin.de/dominikschroeck/master-thesis/tree/master/src/benchmark_libraries)

Here you will find the custom serializers for improving throughput.

## TODO: Benchmarks and more