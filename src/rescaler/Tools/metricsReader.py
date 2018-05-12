import whisper
import time
import glob
from conf import config
import csv


# Rather complex class for reading Whisper files into Pyhton objects
# Mix of two approaches: 1) Deprecated, and old: Reading into a Dict
# 2) Reading into the classes Metric and Operator -> New and preferred way
class MetricsReader:
    uber_list = []

    # Just a small lambda for removing empty values
    def drop_nulls(self, list):
        return [x for x in list if x is not None]



    # USE!
    def read_metrics_whisper_obj(self, metrics, start_path, operators=config.operators, level = 0):
        now = int(time.time())
        yesterday = now - (60 * config.metric_interval)

        for filename in glob.iglob(start_path + '**/*', recursive=True):
            if filename.endswith(".wsp"):

                # gehe durch operatoren in config.operators
                for operator in config.operators:
                    # Nehme Key aus dem Metric Objekt
                    if operator.lower() in filename.lower() and not "overall" in filename.lower():


                        for meter in metrics[operator].get_metrics():
                            list = self.drop_nulls(whisper.fetch(filename, yesterday, now)[1])

                            if meter.get_name().lower() in filename.lower():
                                if len(list) > 0:
                                    if "Parallelism".lower() in meter.get_name().lower():
                                        value = max(list)
                                        meter.set_value(value)
                                    else:
                                        average = sum(list) / len(list)
                                        summed = meter.get_value() + average
                                        meter.set_list(list)
                                        if summed > 0:
                                            meter.set_value(summed)

                    elif "overall" in operator.lower() and "overall" in filename.lower() and operator.lower() in filename.lower():
                        for meter in metrics[operator].get_metrics():
                            list = self.drop_nulls(whisper.fetch(filename, yesterday, now)[1])

                            if meter.get_name().lower() in filename.lower():

                                if len(list) > 0:
                                    average = sum(list) / len(list)
                                    summed = meter.get_value() + average
                                    meter.set_list(list)
                                    if summed > 0:
                                        meter.set_value(summed)
            else:
                if level < 100:
                    self.read_metrics_whisper_obj(metrics, filename, level + 1)

                                # We found the meter. So now extract and store in meter



    # DEPRECATED BUT REQUIRED FOR SYSTEM METRICS, AS OF NOW. Did not want to use "Metric" and "Operator" Classes for the System Metrics
    def read_metrics_whisper(self, metrics, start_path, taskmanagers, level=0):
        now = int(time.time())
        managers = []
        for man in taskmanagers:
            managers+=[man[0]]
        yesterday = now - (60 * config.metric_interval)
        for filename in glob.iglob(start_path + '**/*', recursive=True):
            if filename.endswith(".wsp"):

                for key in metrics.keys():
                    if key in filename:
                        #print(key)

                        # if not "jobmanager" in filename and not "Status/JVM" in key: # Ignore jobmanager metrics
                        if not key.lower().endswith("throughput"):

                            list = self.drop_nulls(whisper.fetch(filename, yesterday, now)[1])

                            if "statesize" in key.lower():

                                if "ibm-power-1" in filename.lower() and len(list) > 0:
                                    average = sum(list) / len(list)
                                    metrics[key][0] = metrics[key][0] + average

                            elif len(list) > 0:
                                average = sum(list) / len(list)
                                metrics[key][0] = metrics[key][0] + average






            else:
                if level < 10:
                    self.read_metrics_whisper(metrics=metrics, start_path=filename, taskmanagers=taskmanagers,level=level + 1)

    # DEPRECATED
    def cleanup_latency_metrics(self,metrics):
        for key in metrics:
            if "latency" in key.lower() or "waitingtime" in key.lower():
                metrics[key][0] = metrics[key][0] / metrics[key][2] # Value divided by Parallelism


    # STILL USED FOR SYSTEM METRICS
    def read_metrics_whisper_to_csv(self, metrics, start_path, taskmanagers, level=0,now=int(time.time())):
        # Setting tiem interval to check. Setting it here makes sure to evaluate the same time for every metric, regardless how long loading takes
        #now = int(time.time())

        yesterday = now - (60 * 300)
        for filename in glob.iglob(start_path + '**/*', recursive=True):
            if filename.endswith(".wsp"):

                for key in metrics.keys():
                    if key in filename:

                        if not key.lower().endswith("throughput") and not key.lower().endswith("latency"):
                            print(key)
                            list = whisper.fetch(filename, yesterday, now)[1]
                            list = [0 if v is None else v for v in list]

                            file = open("results/" + filename.replace("/", "")[-80:] + ".csv", "w")
                            print(filename)
                            writer = csv.writer(file, delimiter=',',
                                                quotechar='|', quoting=csv.QUOTE_MINIMAL)

                            #list.insert(0, config.job_name + "." + key)
                            if "statesize" in key.lower():

                                if "power-1" in filename.lower() and len(list) > 0:
                                    writer.writerow([key])
                                    i = 0
                                    for item in list:
                                        i = i + 1
                                        writer.writerow([item])
                                        file.flush()
                                    print("wrote " + str(i) + " lines for " + str(key))

                            else:
                                writer.writerow([key])
                                i = 0
                                for item in list:
                                    i = i + 1
                                    writer.writerow([item])
                                    file.flush()
                                print("wrote " + str(i) + " lines for " + str(key))

                            file.close()
            else:
                if level < 10:

                    self.read_metrics_whisper_to_csv(metrics, filename,taskmanagers=taskmanagers, level=level + 1,now=now)

    # USE
    def read_metrics_whisper_obj_to_csv(self, metrics, start_path, level = 0,now=int(time.time())):
        #now = int(time.time())
        yesterday = now - (60 * 300)
        for filename in glob.iglob(start_path + '**/*', recursive=True):
            if filename.endswith(".wsp"):

                # gehe durch operatoren in config.operators
                for operator in config.operators:
                    # Nehme Key aus dem Metric Objekt
                    if operator.lower() in filename.lower():

                        for meter in metrics[operator].get_metrics():
                            if meter.get_name().lower() in filename.lower():
                                #if "throughput" in meter.get_name().lower(): print(operator + "," +meter.get_name())
                                list = whisper.fetch(filename, yesterday, now)[1]
                                list = [0 if v is None else v for v in list]

                                if "throughput" in meter.get_name().lower() and len(list) > 0:
                                    average = sum(list) / len(list)
                                    summed = meter.get_value() + average
                                    meter.set_value(summed)
                                    #print(meter.get_name() + ","+ operator + "," + str(summed))

                                if "numrecordsinpersecond" in meter.get_name().lower() and len(list) > 0:
                                    average = sum(list) / len(list)
                                    summed = meter.get_value() + average
                                    meter.set_value(summed)

                                elif "latency" in meter.get_name().lower() or "waitingtime" in meter.get_name().lower() and len(list) > 0:
                                    average = 0
                                    if len(list)>0:
                                        average = sum(list) / len(list)
                                    summed = meter.get_value() + average
                                    meter.set_value(summed)

                                elif "statesize" in meter.get_name().lower() and len(list) > 0:
                                    average = 0

                                    if len(list) > 0:
                                        average = sum(list) / len(list)
                                    summed = meter.get_value() + average
                                    meter.set_value(summed)


                                elif len(list)>0:
                                    average = sum(list) / len(list)
                                    summed = meter.get_value() + average
                                    meter.set_value(summed)

                                # ACTUAL WRITING OUT THE METRIC
                                file = open("results/" + filename.replace("/", "-")[-80:] + ".csv", "w")
                                #print(filename)
                                writer = csv.writer(file, delimiter=',',
                                                    quotechar='|', quoting=csv.QUOTE_MINIMAL)

                                # list.insert(0, config.job_name + "." + key)

                                writer.writerow([str(operator) + "-" +str(meter.get_name())])
                                i = 0
                                for item in list:
                                    i = i + 1
                                    writer.writerow([item])
                                    file.flush()


                                file.close()
            else:
                if level < 10:
                    self.read_metrics_whisper_obj_to_csv(metrics, filename, level + 1)
