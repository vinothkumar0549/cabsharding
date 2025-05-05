package com.example.pojo;

public class TotalSummary {

    private int userid;
    private int trips;
    private int fare;
    private int commission;

    public TotalSummary(int userid, int trips, int fare, int commission) {
        this.userid = userid;
        this.trips = trips;
        this.fare = fare;
        this.commission = commission;
    }

    public TotalSummary(int userid, int trips, int fare) {
        this.userid = userid;
        this.trips = trips;
        this.fare = fare;
    }

    public int getUserid() {
        return userid;
    }

    public void setUserid(int userid) {
        this.userid = userid;
    }

    public int getTrips() {
        return trips;
    }

    public void setTrips(int trips) {
        this.trips = trips;
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
        sb.append(userid).append(", ");
        sb.append(trips).append(", ");
        sb.append(fare).append(", ");
        sb.append(commission);
        return sb.toString();
    }

    
    
}
