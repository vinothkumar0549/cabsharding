package com.example.pojo;

public class Ride {
    
    private int rideid;
    private int customerid;
    private int cabid;
    private String source;
    private String destination;
    private int fare;
    private int commission;

    public Ride() {
        
    }

    public Ride(int cabid, String source, String destination, int fare) {
        this.cabid = cabid;
        this.source = source;
        this.destination = destination;
        this.fare = fare;
    }

    public Ride(int customerid, String source, String destination, int fare, int commission) {
        this.customerid = customerid;
        this.source = source;
        this.destination = destination;
        this.fare = fare;
        this.commission = commission;
    }

    public int getRideid() {
        return rideid;
    }

    public void setRideid(int rideid) {
        this.rideid = rideid;
    }

    public int getCustomerid() {
        return customerid;
    }

    public void setCustomerid(int customerid) {
        this.customerid = customerid;
    }

    public int getCabid() {
        return cabid;
    }

    public void setCabid(int cabid) {
        this.cabid = cabid;
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

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public int getFare() {
        return fare;
    }

    public void setFare(int fare) {
        this.fare = fare;
    }

    public int getCommission() {
        return commission;
    }

    public void setCommission(int commission) {
        this.commission = commission;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(rideid).append(", ");
        sb.append(customerid).append(", ");
        sb.append(cabid).append(", ");
        sb.append(source).append(", ");
        sb.append(destination).append(", ");
        sb.append(fare).append(", ");
        sb.append(commission);
        return sb.toString();
    }
    

    
}
