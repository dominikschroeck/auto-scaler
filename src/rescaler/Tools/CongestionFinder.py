from conf import config


class CongestionFinder:

    def evaluateLatency(self, metrics, accepted_latency, queries):
        # Going query-wise because I need to evaluate the Operator Latency vs Overall Latency
        for query in queries:

            overall_suffix_lat = "-OverallLatency"
            overall_suffix_thru = "-OverallThroughput/m1_rate"

            overall_lat = 0.0
            overall_thru = 0.0

            # Get Overall Latency
            for key in metrics.keys():
                if key.endswith(overall_suffix_lat) and query in key:
                    overall_lat = metrics[key][0]

            # Simple Evaluation of Accepted Latency
            for key in metrics.keys():
                if query in key:
                    if key.endswith("Latency_1") or key.endswith("Latency_2") or key.endswith("Latency"):

                        if metrics[key][0] > accepted_latency and not "overall" in key.lower():
                            metrics[key][1] = metrics[key][1] + 1
                            print("Latency violation: " + key + ", " + str(metrics[key][0]))

            # Contribution to Latency of Query
            for key in metrics.keys():
                if query in key:
                    if not key.endswith(overall_suffix_thru) and "latency" in key.lower() and metrics[key][
                        0] > 0 and not "overall" in key.lower():

                        contribution = 0.0
                        if overall_lat > 0:
                            contribution = metrics[key][0] / overall_lat
                        if overall_lat > accepted_latency and contribution >= 0.5:
                            metrics[key][1] = metrics[key][1] + 1
                            print("Latency Contribution violation: " + key + ", " + str(contribution))

                    if "waitingtime" in key.lower() and metrics[key][0] > 0:

                        contribution = 0.0
                        if overall_lat > 0:
                            contribution = metrics[key][0] / overall_lat

                        if overall_lat > accepted_latency and contribution >= 0.5:
                            metrics[key][1] = metrics[key][1] + 1
                            print("WaitingTime Contribution violation: " + key + ", " + str(contribution))

    def findCongested(self, accepted_latency, metrics, operators):
        queries = []
        if config.benchmark == "nexmark":
            queries = ["Query4", "Query5", "Query6", "Query7", "Query8"]

        # print(operators)

        # Get input rates for each operator. We need the config.sources dict here!
        operator_inputs = {}
        for operator in operators:
            input_rate = 0
            if not "input" in operator.lower():
                list_inputs = config.sources[operator]
                for input in list_inputs:
                    input_rate = input_rate + metrics[input].get_metric("m1_rate")
            else:
                input_rate = metrics[input].get_metric("m1_rate")
            operator_inputs[operator] = input_rate

        ##########################
        ##        LATENCY       ##
        ##########################

        self.evaluateLatency(metrics, accepted_latency, queries)

        ##############################
        ##         THROUGHPUT       ##
        ##############################

        self.analyze_throughput(metrics, operators, operator_inputs)

        ##############################
        ##         ALIGNMENT        ##
        ##############################

        self.analyze_alignment(metrics, operators, 100000)

        ##############################
        ## COMBINE Congestion_Level ##
        ##############################

        operator_list = []
        all_operators = []
        for operator in operators:
            parallelism = 0  # Obtain from metrics
            Congestion_Level = 0.0
            for key in metrics.keys():
                if operator in key and ("throughput_1" not in key.lower() or "throughput_2" not in key.lower()):
                    parallelism = metrics[key][2]
                    Congestion_Level = Congestion_Level + metrics[key][1]

            if Congestion_Level > 0:
                operator_list.append((operator, Congestion_Level, parallelism))
            all_operators.append((operator, Congestion_Level,
                                  parallelism))  # We require this list in case we want to scale out and not only scale up!
        print(metrics)

        # So now we have a Congestion_Leveling: Congestion_Level = 4 means relative latency contribution, absolute latency, throughput and waitingtime are shitty
        # However, overall measures can only have a mraximum Congestion_Level of 3 - no waitingtime measure!
        # Only analyzing the operators that already expose a high latency contribution
        print("Congested Operators")
        print(operator_list)
        print("Input rates")
        print(operator_inputs)

        return operator_list, operator_inputs, all_operators

    #### Analyze the throughput ####
    def analyze_throughput(self, operator_metrics, operators, operator_inputs):
        # Analyse Throughput. Which operator is so slow that we might want to scale out the operator.

        # Iterate operators and compute the ratio throughput to input. We find the operator that slows down the exeuction of the query
        for operator in operators:
            for meter in operator_metrics.keys():
                throughput_by_input = 0
                if operator in meter and (meter.endswith("Throughput") or meter.endswith("Throughput/m1_rate")):
                    if operator_inputs[operator] > 0:
                        throughput_by_input = operator_metrics[meter][0] / (operator_inputs[operator])

                        if throughput_by_input < 0.5 and throughput_by_input > 0.0:
                            operator_metrics[meter][1] = operator_metrics[meter][1] + 1



    # Analyze Alignment time. The result is that we probably have to rescale the slowest previous operator
    def analyze_alignment(self, operator_metrics, operators, alignmnent_threshold):
        for operator in operators:
            for meter in operator_metrics.keys():
                if "alignment" in meter.lower() and operator.lower() in meter.lower():
                    alignment = operator_metrics[meter][0]
                    if alignment > alignmnent_threshold:  # Alignment time threshold
                        # Actually rescale the previous operator(s), namely the slowest ones, but how do we identify slowness now? compare their throughput?
                        predecessors = config.predecessors[operator]  # [PREDECESSOR, PREDECESSOR...]
                        slowest_operator = ""
                        lowestThroughput = 0
                        for predecessor in predecessors:  # Obtain the throughputs
                            currentthroughput = operator_metrics[str(predecessor + "-Throughput")]
                            if lowestThroughput == 0 or currentthroughput < lowestThroughput:
                                lowestThroughput = operator_metrics[str(predecessor + "-Throughput")][0]
                                slowest_operator = predecessor

                        # Now we know which one of the predecessors is slowest, let us now increase the congestion level for it
                        operator_metrics[slowest_operator + "-Throughput"] = operator_metrics[
                                                                                 slowest_operator + "-Throughput"] + 1
