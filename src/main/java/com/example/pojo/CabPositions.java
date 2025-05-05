package com.example.pojo;

public class CabPositions {
    
    private String locationname;
    private String cabid;

    public CabPositions(String locationname, String cabid) {
        this.locationname = locationname;
        this.cabid = cabid;
    }
    
    public String getLocationname() {
        return locationname;
    }
    public void setLocationname(String locationname) {
        this.locationname = locationname;
    }
    public String getCabid() {
        return cabid;
    }
    public void setCabid(String cabid) {
        this.cabid = cabid;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(locationname).append(", ");
        sb.append(cabid).append(", ");
        return sb.toString();    }
    
}
