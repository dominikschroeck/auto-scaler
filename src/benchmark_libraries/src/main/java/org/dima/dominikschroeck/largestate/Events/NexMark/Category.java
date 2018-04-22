package org.dima.dominikschroeck.largestate.Events.NexMark;

public class Category {
    private String name;
    private String description;
    private Integer id;

    public Category(String name, String description, Integer id) {
        this.name = name;
        this.description = description;
        this.id = id;
    }

    @Override
    public String toString() {
        return "Category{" +
                "name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", id=" + id +
                '}';
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public Integer getId() {
        return id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setId(Integer id) {
        this.id = id;
    }
}
