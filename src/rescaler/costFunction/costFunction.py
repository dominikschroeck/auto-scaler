from conf import config
from Tools.util import cluster_health_index
from Tools.util import cluster_memory_index
import math
from Tools.configWriter import ConfigWriter


class cost:

    # Algorithm for finding the "optimal" Parallelism of an operator using throughput, input rate and parallelism
    def get_optimal_parallelism(self, operator, throughput, input_rate, parallelism):
        optimal_parallelism = 0

        # Ignore AllWindow Operators
        if "allwindow" in operator.lower():
            print("Doing nothing with allwindow")
            optimal_parallelism = 1

        # For Join, CoGroup and (Parallel) Windows: parallelism + 2
        elif "join" in operator.lower() or "cogroup" in operator.lower() or operator in config.map_joins or "window" in operator.lower():

            optimal_parallelism = parallelism + 2

        # For all Continuous operators: Use heuristic
        elif throughput > 0:
            optimal_parallelism = math.ceil(input_rate / (throughput / parallelism))

        # Fallback
        else:

            print("Throughput = 0 for operator " + operator + "! Adding two channels")
            optimal_parallelism = parallelism + 2

        return optimal_parallelism

    # Actual Rescale Decision-Making: Definitely Scale-Up, possibly Scale-Out!
    def evaluateCost(self, Metrics, System_Metrics, Operators=config.operators):
        print("----------------------------------------------------------------------------")

        list_operators_latency = {}
        for operator in Operators:
            latency = Metrics[operator].get_latency()
            throughput = Metrics[operator].get_metric("Throughput/m1_rate")
            parallelism = Metrics[operator].get_parallelism()

            input_rate = Metrics[operator].get_metric("numRecordsInPerSecond/m1_rate")

            # This approach ignores allWindow operators, because here we cannot really rescale


            if not "overall" in operator.lower() and not "allwindow" in operator.lower(): # We ignore AllWindow operators
                list_operators_latency[operator] = [latency, throughput, parallelism, input_rate]


        ####################################
        ##          SCALE-UP Process      ##
        ####################################

        sorted_operators = [y[1] for y in
                            sorted([(list_operators_latency[x][0], x) for x in list_operators_latency.keys()])]

        # Taking the k slowest operators as defined in config
        sorted_operators = sorted_operators[-config.number_to_scale:]

        rescaled_operators = {}

        sum_throughput = 0
        sum_input_rates = 0
        print("All rescaled Operators: ")


        for operator in sorted_operators:
            latency = Metrics[operator].get_latency()
            throughput = Metrics[operator].get_metric("Throughput/m1_rate")
            parallelism = Metrics[operator].get_parallelism()

            if "cogroup" in operator.lower() or "window" in operator.lower():

                input_rate = Metrics[operator].get_metric("WindowSize/m1_rate")
            else:
                input_rate = Metrics[operator].get_metric("numRecordsInPerSecond/m1_rate")

            sum_input_rates = sum_input_rates + input_rate
            sum_throughput = sum_throughput + throughput

            # Change Overall Parallelism if CoGroup or Join
            if "join" in operator.lower() or "cogroup" in operator.lower() or operator in config.map_joins:
                rescaled_operators["parallelism"] = self.get_optimal_parallelism(operator=operator,
                                                                                 throughput=throughput,
                                                                                 input_rate=input_rate,
                                                                                 parallelism=parallelism)
                print(operator + ", Latency = " + str(latency) + ", throughput= " + str(throughput) + ", input rate= " + str(input_rate) + " , parallelism= " + str(parallelism) + ", new parallelism= " + str(rescaled_operators["parallelism"]))

            else:
                rescaled_operators[operator] = self.get_optimal_parallelism(operator=operator, throughput=throughput,
                                                                            input_rate=input_rate,
                                                                            parallelism=parallelism)
                print(operator + ", Latency = " + str(latency) + ", throughput= " + str(throughput) + ", input rate= " + str(input_rate) + " , parallelism= " + str(parallelism) + ", new parallelism= " + str(rescaled_operators[operator]))


        ####################################
        ##          SCALE-OUT Process     ##
        ####################################

        # Load Indices (in %, overall Cluster)
        health_index = cluster_health_index(System_Metrics)
        memory_index = cluster_memory_index(System_Metrics)


        # Find State Size metric
        stateSize = 0
        for metric in System_Metrics.keys():
            if "stateSize" in metric:
                stateSize = stateSize + System_Metrics[metric][0]


        # Downtime (ONLY SCALE OUT) depends on State Migration time.
        offtime_duration = (stateSize / config.max_network) * 2


        print("Offtime duration: " + str(offtime_duration))
        print("StateSize: " + str(stateSize))


        congested = False


        # Decision Making:

        if memory_index >= 0.7 and offtime_duration < config.max_downtime: # Identify Memory Congestion
            congested = True


        if health_index > 0.5 and offtime_duration < config.max_downtime:
            congested = True

        if health_index >= 0.7:
            congested = True

        # -------------------------------------------------------
        # Make Decision

        if not congested:
            print("NO SCALE OUT, JUST SCALE UP!")
            optimized = (rescaled_operators, False)
            return optimized
        else:
            print("WE WANT TO SCALE OUT AS WELL")
            optimized = (rescaled_operators, True)
            return optimized
