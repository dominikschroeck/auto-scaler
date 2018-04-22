package org.dima.dominikschroeck.largestate.kafka.Jobs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import java.util.HashMap;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

/**
 * Main Class. Reads the configuration and starts the producer threads.
 */
public class Job_starter {


    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws FileNotFoundException {
        Yaml yaml = new Yaml();
        int parallelism = 1;
        String kafka_server = "";
        String kafka_zookeeper = "";
        int checkpointing_interval = 10000;
        int events_per_second_max = 1000000;
        int events_per_second_low = 1;
        int change_interval = 10000;
        Long pause = 1L;
        String job = "NONE";
        Producer_Job producer_job = new Producer_Job() {
            @Override
            void runJob() {
                super.runJob();
                System.out.println("[INFO] No valid job setting found in Conf! Exiting");
            }
        };
        Map<String, String> values = new HashMap<>();
        try{
        values = (Map<String, String>) yaml
                .load(new FileInputStream(new File(args[0])));}
        catch (ArrayIndexOutOfBoundsException e){

        }


        for (String key : values.keySet()) {
            if (key.equals("parallelism")) {
                parallelism = Integer.parseInt(values.get(key));
                System.out.println("[INFO] Setting parallelism to: " + parallelism);
            }
            if (key.equals("kafka_server")) {
                kafka_server = values.get(key);
                System.out.println("[INFO] Kafka Source to: " + kafka_server);
            }
            if (key.equals("kafka_zookeeper")) {
                kafka_zookeeper = values.get(key);
                System.out.println("[INFO] Setting Kafka Zookeeper to: " + kafka_zookeeper);
            }
            if (key.equals("events.perstep.max")) {
                events_per_second_max = Integer.parseInt(values.get(key));
                System.out.println("[INFO] Max Events to: " + events_per_second_max);
            }
            if (key.equals("events.perstep.low")) {
                events_per_second_low = Integer.parseInt(values.get(key));
                System.out.println("[INFO] Min Events to: " + events_per_second_low);
            }

            if (key.equals("pause")) {
                pause = Long.parseLong(values.get(key));
                System.out.println("[INFO] Pause length between production: " + pause);
            }

            if (key.equals("change_interval")) {
                change_interval = Integer.parseInt(values.get(key));
                System.out.println("[INFO] Change of Events interval: " + change_interval);
            }

            if (key.equals("benchmark")) {
                job = values.get(key);
            }
        }
        if (kafka_server == "" || kafka_zookeeper == "") {
            System.out.println("[INFO] No Kafka Server Settings found in Config file! Exiting.");

        } else {
            System.out.println("[INFO] Start Producing for Job: " + job);
            if (job.equals("nexmark")) {

               producer_job = new NexMark_Job(kafka_server,kafka_zookeeper,parallelism,events_per_second_max,events_per_second_low,pause,change_interval);
            } else if (job.equals("psm")) {
                producer_job = new PSM_Job(kafka_server,kafka_zookeeper,parallelism,events_per_second_max,events_per_second_low,pause,change_interval);
            }

        }
        producer_job.runJob();
    }






}
