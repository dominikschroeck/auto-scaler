package org.dima.dominikschroeck.largestate.Events.NexMark;


import java.io.Serializable;

public class NewItemEvent implements Serializable {
    public Long timestamp;
    public Integer category_id;
    public String name;
    public String description;
    public Integer item_id;
    public Long ingestion_timestamp;

    public NewItemEvent() {
    }

    public NewItemEvent(Long timestamp, Integer category_id, String name, String description, Integer item_id, Long ingestion_timestamp) {
        this.timestamp = timestamp;
        this.category_id = category_id;
        this.name = name;
        this.description = description;
        this.item_id = item_id;
        this.ingestion_timestamp = ingestion_timestamp;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public Long getIngestion_timestamp() {
        return ingestion_timestamp;
    }

    public void setIngestion_timestamp(Long ingestion_timestamp) {
        this.ingestion_timestamp = ingestion_timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Integer getCategory_id() {
        return category_id;
    }

    public void setCategory_id(Integer category_id) {
        this.category_id = category_id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }


    public Integer getItem_id() {
        return item_id;
    }

    public void setItem_id(Integer item_id) {
        this.item_id = item_id;
    }

    //"NewItemEvent," + timestamp + "," + item_id + "," + category_id + "," + name + "," + description + "," ;

}