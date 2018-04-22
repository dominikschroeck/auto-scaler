package org.dima.dominikschroeck.largestate.Events.NexMark;


import java.io.Serializable;

public class NewPersonEvent implements Serializable {

    public Long timestamp;
    public Integer person_id;
    public String email;
    public Integer creditcard;
    public String city;
    public String state;
    public Long ingestion_timestamp;
    public String name;

    public NewPersonEvent() {
    }

    public NewPersonEvent(Long timestamp, Integer person_id, String name, String email, Integer creditcard, String city, String state, Long ingestion_timestamp) {
        this.timestamp = timestamp;
        this.person_id = person_id;
        this.email = email;
        this.creditcard = creditcard;
        this.city = city;
        this.state = state;
        this.ingestion_timestamp = ingestion_timestamp;
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getTimestamp() {
        return timestamp;
    }


    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }


    public Integer getPerson_id() {
        return person_id;
    }


    public void setPerson_id(Integer person_id) {
        this.person_id = person_id;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public Integer getCreditcard() {
        return creditcard;
    }

    public void setCreditcard(Integer creditcard) {
        this.creditcard = creditcard;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }


    public Long getIngestion_timestamp() {
        return ingestion_timestamp;
    }


    public void setIngestion_timestamp(Long ingestion_timestamp) {
        this.ingestion_timestamp = ingestion_timestamp;
    }
}
