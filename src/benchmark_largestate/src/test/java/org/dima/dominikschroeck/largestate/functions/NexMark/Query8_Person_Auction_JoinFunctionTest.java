package org.dima.dominikschroeck.largestate.functions.NexMark;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.dima.dominikschroeck.largestate.functions.NexMark.Query8_Person_Auction_JoinFunction;
import org.junit.Test;
import org.junit.Assert;

public class Query8_Person_Auction_JoinFunctionTest  {

    @Test
    public void testJoin() {
        Query8_Person_Auction_JoinFunction function = new Query8_Person_Auction_JoinFunction(2);
        //Out: currentTime, person_id, name
        // in: Person:  timestamp + "," + id + "," + email + "," + name + "," + credit_card + "," + city + "," + state ;
        // in: Auction:  timestamp + "," + auction_id + "," + person_id + "," + item_id + "," + initialPrice + "," +  reserve + "," + start + "," + end;
        Tuple7<Long, Long, String, String, Long, String, String> person = new Tuple7<>(1L,1L,"Name@google.com","Name",4263L,"Berlin","Baerlin");
        Tuple8<Long, Long, Long, Long, Double, Double, Long, Long> auction = new Tuple8<>
                (1L,1L,1L,1L,10D,5D,1L,60L);

       /* try {
            Tuple3<Long,Long, String> result = function.join(person,auction);
            Assert.assertTrue(result.f1 == person.f1);
            Assert.assertTrue(result.f2 == person.f3);
        } catch (Exception e) {

        }*/




    }



}