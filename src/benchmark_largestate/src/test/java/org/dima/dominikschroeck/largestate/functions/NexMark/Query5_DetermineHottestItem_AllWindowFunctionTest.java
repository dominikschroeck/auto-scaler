package org.dima.dominikschroeck.largestate.functions.NexMark;


import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

public class Query5_DetermineHottestItem_AllWindowFunctionTest {
    TimeWindow window;
    Query5_DetermineHottestItem_AllWindowFunction function;
    Collector<Tuple5<Long, Integer, Integer, Long, Integer>> collector;
    ArrayList<Tuple5<Long, Integer, Integer, Long, Integer>> collector_list;
    ArrayList<Tuple4<Long, Integer, Long, Integer>> iterable;

    @Before
    public void setUp() {
        window = new TimeWindow ( System.currentTimeMillis ( ), System.currentTimeMillis ( ) + 10000L );
        function = new Query5_DetermineHottestItem_AllWindowFunction (1 );
        collector_list = new ArrayList<> ( );

        iterable = new ArrayList<> ( );
        collector = new Collector<Tuple5<Long, Integer, Integer, Long, Integer>> ( ) {
            @Override
            public void collect(Tuple5<Long, Integer, Integer, Long, Integer> longLongLongTuple3) {
                collector_list.add ( longLongLongTuple3 );
            }

            @Override
            public void close() {

            }
        };
    }

    /*@Test
    public void apply() throws Exception {
        //iterable.add(new Tuple2<>(1000L,1L));
        int items = 100;
        int[] ids = new int[items];
        int[] counts = new int[items];
        System.out.println("Generate Data");
        for (int i = 0;i<items;i++){
            ids[i] = i;
            counts[i] = ThreadLocalRandom.current ().nextInt ( 1,27 );
        }
       // int[] ids = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
       // int[] counts = {1, 23, 45, 12, 45, 12, 12, 12, 45, 45, 45};

        System.out.println("findMax");
        int expected_result = findMax ( counts );
        System.out.println("findCount");
        int expected_noResults = countOccurence ( counts, expected_result );


        System.out.println("Fill iterable" );

        for (int i = 0; i < ids.length; i++) {
            for (int j = 0; j < counts[i]; j++) {
                iterable.add ( new Tuple4<> ( System.currentTimeMillis ( ), ids[i], 34123414L, 23 ) );
            }
        }

        System.out.println ( "Starting Actual Test" );
        Long start = System.currentTimeMillis () ;
        function.apply ( null, iterable, collector );
        Long latency = System.currentTimeMillis () - start;
        System.out.println ("took " + latency +  " ms to execute");

        Assert.assertTrue ( collector_list.size ( ) == expected_noResults );

        Tuple5<Long, Integer, Integer, Long, Integer>[] results = new Tuple5[expected_noResults];
        for (int k = 0; k < expected_noResults; k++) {
            Tuple5<Long, Integer, Integer, Long, Integer> result = collector_list.get ( k );
            Assert.assertTrue ( result.f2 == expected_result );
        }

    }*/

    private int findMax(int[] arr) {
        int max = 0;
        for (int i = 0; i < arr.length; i++) {
            if (arr[i] > max){
                max = arr[i];
            }
        }
        return max;
    }

    private int countOccurence(int[] arr, int value){
        int count = 0;
        for (int i = 0; i < arr.length; i++) {
            if (arr[i] == value){
                count++;
            }
        }
        return count;
    }


}