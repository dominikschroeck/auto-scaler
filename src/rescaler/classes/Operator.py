import conf.config as config
from classes.Metric import Metric


class Operator:
    __name = ""
    __metrics = []
    __parallelism = 0


    def add_metric(self, metric_name, value=0.0):
        metric = Metric(metric_name, value)
        self.__metrics = self.__metrics + [metric]

    def get_metric(self, name):
        for metric in self.__metrics:
            if metric.get_name() == name:
                return metric.get_value()
                break
        return None

    def get_metric_object(self, name):
        for metric in self.__metrics:
            if metric.get_name() == name:
                return metric
                break
        return None

    def remove_metric(self, metric_name):
        for metric in self.__metrics:
            if metric.get_name() is metric_name: self.__metrics.remove(metric)

    def get_latency(self):
        if "overall" in self.__name.lower():
            return self.get_metric("Latency")
        elif "input" in self.__name.lower():
            return 0
        else:  # Else we deal with an ordinary operator
            return (self.get_metric("Latency") + self.get_metric("WaitingTime"))

    def get_metrics(self):
        return self.__metrics

    def get_parallelism(self):
        return self.__parallelism

    def get_name(self):
        return self.__name

    def set_parallelism(self, parallelism):
        self.__parallelism = parallelism

    def __init__(self, name, parallelism=0):
        self.__name = name
        self.__parallelism = parallelism

    # This method is required to correctly compute the average latency
    # First, divide WaitingTime/Latency_1 / _2 by the parallelism
    # Then, sum up the values and divide by 2
    def clean_metrics(self):
        latency = 0.0
        waitingTime = 0.0



        if "coprocess" in self.__name.lower():
            self.get_metric_object("WaitingTime_1").set_value(self.get_metric("WaitingTime_1") / self.__parallelism)
            self.get_metric_object("WaitingTime_2").set_value(self.get_metric("WaitingTime_2") / self.__parallelism)
            waitingTime = (self.get_metric("WaitingTime_1") + self.get_metric("WaitingTime_2")) / 2
            self.get_metric_object("WaitingTime").set_value(waitingTime)


            self.get_metric_object("Latency_1").set_value(self.get_metric("Latency_1") / self.__parallelism)
            self.get_metric_object("Latency_2").set_value(self.get_metric("Latency_2") / self.__parallelism)
            latency = (self.get_metric("Latency_1") + self.get_metric("Latency_2")) / 2
            self.get_metric_object("Latency").set_value(latency)

            self.remove_metric("Latency_1")
            self.remove_metric("Latency_2")
            self.remove_metric("WaitingTime_1")
            self.remove_metric("WaitingTime_2")

        elif "join" in self.__name.lower() or self.__name in config.map_joins:

            self.get_metric_object("WaitingTime_1").set_value(self.get_metric("WaitingTime_1") / self.__parallelism)
            self.get_metric_object("WaitingTime_2").set_value(self.get_metric("WaitingTime_2") / self.__parallelism)

            waitingTime = (self.get_metric("WaitingTime_1") + self.get_metric("WaitingTime_2")) / 2

            self.get_metric_object("WaitingTime").set_value(waitingTime)

            self.remove_metric("WaitingTime_1")
            self.remove_metric("WaitingTime_2")


            latency = self.get_metric("Latency")
            self.get_metric_object("Latency").set_value(latency / self.__parallelism)

        else:
            # For all others we also have to divide by the actual parallelism in order to compute the correct latency
            # Because we deal with a SUM. For evaluation, we need the AVG because the operators run in parallel!
            waitingTime_obj = self.get_metric_object("WaitingTime")
            if waitingTime_obj is not None:
                waitingTime = waitingTime_obj.get_value()
                waitingTime_obj.set_value(waitingTime / self.__parallelism)

            latency = self.get_metric("Latency")

            self.get_metric_object("Latency").set_value(latency / self.__parallelism)
