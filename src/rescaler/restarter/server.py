import socket  # Import socket module
import yaml
import os
import time
import requests
import requests



available_taskManagers = ["ibm-power-8","ibm-power-9","ibm-power-7"]

def scale():
    stream = yaml.load(open('received_file', "r"))
    # Adding a node if neccessary
    if stream["out"] == "out":
        print("\n")
        print("----------------------------")
        print("ADDING ANOTHER TASKMANAGER")

        os.system("ssh " + available_taskManagers[0] + " ~/dschroeck_thesis/flink-Metrics/bin/taskmanager.sh start")
        available_taskManagers.pop(0)


    print("--------------------------------")
    print('Updated configuration received')
    print('Cancelling and Creating Savepoint')

    ####
    savepoint_folder = "hdfs://ibm-power-1:44000/user/dschroeck/save"

    response = requests.get('http://localhost:8981/jobs')
    data = response.json()
    job_id = data['jobs-running'][0] # Using the first job as JOB_ID for the job to restart.


    cancel_string = "/home/hadoop/dschroeck_thesis/flink-Metrics/bin/flink cancel -s " + savepoint_folder + " " + job_id

    output = os.popen(cancel_string).read()


    really_stopped = False
    print("Waiting for previous Job to fully cancel")
    while not really_stopped:
        response = requests.get('http://localhost:8981/jobs')
        data = response.json()
        if not job_id in data['jobs-running']:
            really_stopped = True
            break

        time.sleep(1)
    print("Job stoppped")
    output.replace("\n","")

    # Search for the actual path in which Flink stored the savepoint

    actual_savepointpath = ""
    start = output.find(savepoint_folder+"/")

    for index in range(len(output) - start - 1):
        actual_savepointpath = actual_savepointpath + output[index + start]
    actual_savepointpath = actual_savepointpath[:-1]



    print("----------------------------")
    print('Restarting job with new config')

    restart_string = "/home/hadoop/dschroeck_thesis/flink-Metrics/bin/flink run -s " + actual_savepointpath + " /home/hadoop/dschroeck_thesis/benchmark.jar received_file"

    os.system(restart_string)

    # Delete Config File from Webserver
    os.system("ssh flink@dominikschroeck.de rm /home/flink/webspace/out.yaml")


    print("\n Rescaling finished. Waiting for new connection")







print('Server checking for configs regularly....')
while True:
    response = requests.get("https://stefanie.dominikschroeck.de/out.yaml")

    if not (b"404 Not Found") in response.content:
        print("Rescale Configuration received!")
        with open('received_file', 'wb') as f:
            f.write(response.content)
        scale()


    #print("Check again in 10 seconds")
    time.sleep(10)

print("Scaling finished,restart me if you like.")



