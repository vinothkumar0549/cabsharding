package com.example.database;

import java.time.LocalDateTime;
import java.util.List;

import com.example.pojo.User;
import com.example.pojo.CabPositions;
import com.example.pojo.CustomerAck;
import com.example.pojo.Penalty;
import com.example.pojo.Ride;
import com.example.pojo.TotalSummary;

public interface Storage {

    public User getUser(String username);

    public boolean login(int userid);

    public boolean logout(int userid);

    public int addUser(User user);

    public boolean addCabLocation(int cabid, int locationid, String cabtype);

    public int checkLocation(String cablocation);

    public int addLocation(String locationname, int distance);

    public String removeLocation(String locationname, int distance);

    public List<CabPositions> checkAvailableCab();

    public CustomerAck getFreeCab(int customerid, String source, String destination, String cabtype, LocalDateTime departuretime, LocalDateTime arrivaltime);

    public boolean addRideHistory(int customerid, int cabid, int distance, String source, String destination, LocalDateTime departuretime, LocalDateTime arrivaltime);

    public boolean cancelRide(int cabid, int customerid);

    public boolean updateCabPositions(int cabid, int locationid);

    public List<Ride> getCustomerRideSummary(int customerid);

    public List<Penalty> getPenalty(int customerid);

    public List<Ride> getCabRideSummary(int cabid);

    public List<List<Ride>> getAllCabRides();

    public List<TotalSummary> getTotalCabSummary();

    public List<List<Ride>> getAllCustomerRides();

    public List<TotalSummary> getTotalCustomerSummary();


}
