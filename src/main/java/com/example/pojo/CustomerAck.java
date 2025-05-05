package com.example.pojo;

public class CustomerAck {

    private int cabid;
    private int distance;
    private int fare;
    private String source;
    private String destination;
    private String roomid;

    public CustomerAck(int cabid, int distance, int fare, String source, String destination) {
        this.cabid = cabid;
        this.distance = distance;
        this.fare = fare;
        this.source = source;
        this.destination = destination;
    }

    public int getCabid() {
        return cabid;
    }

    public void setCabid(int cabid) {
        this.cabid = cabid;
    }

    public int getDistance() {
        return distance;
    }

    public void setDistance(int distance) {
        this.distance = distance;
    }

    public int getFare() {
        return fare;
    }

    public void setFare(int fare) {
        this.fare = fare;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destiantion) {
        this.destination = destiantion;
    }

    public String getRoomid() {
        return roomid;
    }

    public void setRoomid(String roomid) {
        this.roomid = roomid;
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("customerack [");
        sb.append("cabid=").append(cabid).append(", ");
        sb.append("distance=").append(distance).append(", ");
        sb.append("fare=").append(fare).append(", ");
        sb.append("source=").append(source).append(", ");
        sb.append("destination=").append(destination).append(", ");
        sb.append("roomid").append(roomid).append(",");
        sb.append("]");
        return sb.toString();
    }

  
    

}
