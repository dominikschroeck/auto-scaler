package org.dima.dominikschroeck.largestate.jobs;

import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * This class reads the config file given and contains the Main method.
 * Starts a Flink job as given in the config file.
 * <p>
 * This and all other classes of this project have been programmed by Dominik Schroeck. 2018, All rights reserved
 */
public class Job_starter {
    static Integer[] paras = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    static Integer[] nexmark_specific_paras = {0, 0, 0, 0, 0};

    /**
     * Main
     *
     * @param args
     * @throws FileNotFoundException
     */
    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws FileNotFoundException {
        Yaml yaml = new Yaml();
        Integer maxParallelism = 50;
        Properties props = new Properties();
        Properties newProps = new Properties();
        int parallelism = 1;


        Map<String, Object> values = new HashMap<>();
        if (args.length > 0) {
            values = (Map<String, Object>) yaml
                    .load(new FileInputStream(new File(args[0])));
        }

        // Parsing all entries to String!
        Map<String, String> actualvals = new HashMap<>();
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            if (entry.getValue() instanceof String) {
                actualvals.put(entry.getKey(), (String) entry.getValue());
            }
            if (entry.getValue() instanceof Integer) {
                actualvals.put(entry.getKey(), String.valueOf(entry.getValue()));
            }
            if (entry.getValue() instanceof Float) {
                actualvals.put(entry.getKey(), String.valueOf(entry.getValue()));
            }
        }


        props.putAll(actualvals);


        Flink_Job flink_job;
        String job = props.getProperty("benchmark") == null ? "" : props.getProperty("benchmark");
        if (job.equals("nexmark")) {
            flink_job = new NexMark_Job(props, getEnvironment(props));
        } else if (job.equals("psm")) {
            flink_job = new PSM_Job(props, getEnvironment(props));
        }

        // ELSE JUST RUNNING AN EMPTY JOB
        else {

            flink_job = new Flink_Job(props, getEnvironment(props)) {
                @Override
                void runJob() {
                    System.out.println("[INFO] No valid job setting found in Conf! Exiting");
                }
            };
        }

        flink_job.runJob();
    }


    /**
     * Method for generating an Environment
     *
     * @param props
     * @return
     */
    private static StreamExecutionEnvironment getEnvironment(Properties props) {


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Integer max_para = props.getProperty("maxParallelism") == null ? 128 : Integer.parseInt(props.getProperty("maxParallelism"));

        env.setMaxParallelism(max_para);

        // DISABLE CHECKPOINTING IF RUN IN LOCAL MODE


        try {
            Boolean incremental = props.getProperty("checkpointing_method").equals("incremental");
            String checkpoint_dir = props.getProperty("checkpoint_dir") == null ? "file:///tmp" : props.getProperty("checkpoint_dir");

            RocksDBStateBackend backend =
                    new RocksDBStateBackend(checkpoint_dir, incremental);
            backend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);
            env.setStateBackend(backend);

        } catch (IOException e) {
            e.printStackTrace();
        }


        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2); // Only one checkpoint at a time

        Integer minpause = props.getProperty("minPauseBetweenCheckpoints") == null ? 1 : Integer.parseInt(props.getProperty("minPauseBetweenCheckpoints"));
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(minpause);

        Integer interval = props.getProperty("checkpointing_interval") == null ? (int) env.getCheckpointInterval() : Integer.parseInt(props.getProperty("checkpointing_interval"));
        env.enableCheckpointing(interval);

        env.getCheckpointConfig().setCheckpointTimeout(600000);

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.getConfig().setAutoWatermarkInterval(1);


        Integer para = props.getProperty("parallelism") == null ? 4 : Integer.parseInt(props.getProperty("parallelism"));
        env.setParallelism(para);

        Integer latency = props.getProperty("latencyTrackingInterval") == null ? 100 : Integer.parseInt(props.getProperty("latencyTrackingInterval"));
        env.getConfig().setLatencyTrackingInterval(latency);
        return env;
    }

}
