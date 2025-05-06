package com.example.database;

// import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
// import java.sql.Types;
import java.time.LocalDate;
// import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.example.pojo.CabPositions;
import com.example.pojo.CustomerAck;
import com.example.pojo.Penalty;
import com.example.pojo.Ride;
import com.example.pojo.TotalSummary;
import com.example.pojo.User;
import com.example.util.DatabaseConnection;
import com.example.util.Gender;
import com.example.util.Role;
// import com.example.websocket.DriverSocket;
import com.example.websocket.DriverSocket;

public class DatabaseStorage implements Storage {

    private static int userid = 0;
    private static int rideid = 0;

    private static final int shards = 3;
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(5);
    private static final Map<Integer, ScheduledFuture<?>> scheduledTasks = new ConcurrentHashMap<>();

    @Override
    public int addUser(User user) {
        String insertuser = "INSERT INTO users (userid, name, username, password, age, gender, role) VALUES (?,?,?,?,?,?,?)";
        // int generatedUserId = -1;
        userid++;
        try (Connection connection = DatabaseConnection.getUserShardConnection(userid % shards);
             PreparedStatement preparedStatementuser = connection.prepareStatement(insertuser, PreparedStatement.RETURN_GENERATED_KEYS)) {

            preparedStatementuser.setInt(1, userid);
            preparedStatementuser.setString(2, user.getName());
            preparedStatementuser.setString(3, user.getUsername());
            preparedStatementuser.setString(4, user.getEncryptedpassword());
            preparedStatementuser.setLong(5, user.getAge());
            preparedStatementuser.setString(6, user.getGender().name());
            preparedStatementuser.setString(7, user.getRole().name()); 

            int val = preparedStatementuser.executeUpdate();

             if (val != 0) {
                return userid;
            //     // Get the generated primary key (userid)
            //     try (ResultSet generatedKeys = preparedStatementuser.getGeneratedKeys()) {
            //         if (generatedKeys.next()) {
            //             generatedUserId = generatedKeys.getInt(1); // Retrieve the generated ID
            //         }
            //     }
            }

        } catch (SQLException e) {
            e.printStackTrace(); 
        }
        return -1;
    }

    @Override
    public User getUser(String username) {
        String query = "SELECT * FROM users WHERE username = ?";

        try {
            for (Connection connection : DatabaseConnection.getAllUserShardConnections()) {
                try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {

                    preparedStatement.setString(1, username);
                    ResultSet result = preparedStatement.executeQuery();

                    if (result.next()) {
                        return new User(
                            result.getInt("userid"),
                            result.getString("name"),
                            result.getString("username"),
                            result.getString("password"),
                            result.getInt("age"),
                            Gender.valueOf(result.getString("gender")),
                            Role.valueOf(result.getString("role"))
                        );
                    }

                } catch (SQLException e) {
                    e.printStackTrace(); // You could log and continue
                } finally {
                    try {
                        if (connection != null && !connection.isClosed()) connection.close();
                    } catch (SQLException ex) {
                        ex.printStackTrace();
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return null; // No user found in any shard
    }


    @Override
    public boolean login(int userid){
        String query = "UPDATE users SET onlinestatus = ? WHERE userid = ?";

        try (Connection connection = DatabaseConnection.getUserShardConnection(userid % shards);
            PreparedStatement preparedStatement = connection.prepareStatement(query)) {

            preparedStatement.setBoolean(1, true);
            preparedStatement.setInt(2, userid);

            int val = preparedStatement.executeUpdate();

            return val > 0;

        } catch (SQLException e) {
            e.printStackTrace(); 
        }

        return false;
    }

    @Override
    public boolean logout(int userid){
        String query = "UPDATE users SET onlinestatus = ? WHERE userid = ?";

        try (Connection connection = DatabaseConnection.getUserShardConnection(userid % shards);
        PreparedStatement preparedStatement = connection.prepareStatement(query)) {

       preparedStatement.setBoolean(1, false);
       preparedStatement.setInt(2, userid);

       int val = preparedStatement.executeUpdate();

       return val > 0;

        } catch (SQLException e) {
            e.printStackTrace(); 
        }

        return false;
    }

    @Override
    public boolean addCabLocation(int cabid, int  locationid, String cabtype) {

        String cabpositionquery = "INSERT INTO cabpositions(cabid, locationid, cabtype) VALUES(?,?,?)";

        try (Connection connection = DatabaseConnection.getCabShardConnection(cabid % shards);
             PreparedStatement preparedStatementcabposition = connection.prepareStatement(cabpositionquery)) {

            preparedStatementcabposition.setInt(1, cabid);
            preparedStatementcabposition.setInt(2, locationid);
            preparedStatementcabposition.setString(3, cabtype);
            int val = preparedStatementcabposition.executeUpdate();
            return val > 0;

        } catch (SQLException e) {
            e.printStackTrace(); 
        }

        return false;

    }

    public int checkLocation(String cablocation) {
        String locationquery = "SELECT locationid FROM locations WHERE locationname = ?";

        try (Connection connection = DatabaseConnection.getLocationConnection();
        PreparedStatement preparedStatementlocation = connection.prepareStatement(locationquery)) {

            preparedStatementlocation.setString(1, cablocation);
            ResultSet result = preparedStatementlocation.executeQuery();

            if (result.next()) {
                return result.getInt("locationid");
            }

        } catch (SQLException e) {
            e.printStackTrace(); 
        }

        return 0;
    }

    public int addLocation(String locationname, int distance) {
        String query = "INSERT INTO locations(locationname, distance) VALUES (?,?)";
        int generatedUserId = -1;

        try (Connection connection = DatabaseConnection.getLocationConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(query,  PreparedStatement.RETURN_GENERATED_KEYS)) {

       preparedStatement.setString(1, locationname);
       preparedStatement.setInt(2, distance);

       int val = preparedStatement.executeUpdate();

            if (val != 0) {
                // Get the generated primary key (userid)
                try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) {
                    if (generatedKeys.next()) {
                        generatedUserId = generatedKeys.getInt(1); // Retrieve the generated ID
                    }
                }
            }
            return generatedUserId;

        } catch (SQLException e) {
            e.printStackTrace(); 
        }

        return -1;
    }

    public String removeLocation(String locName, int locDistance) {
        String statusMessage;
        int shards = 3;
    
        try ( Connection centralConn = DatabaseConnection.getLocationConnection();){
            // 1. Get loc_id
            PreparedStatement locStmt = centralConn.prepareStatement(
                "SELECT locationid FROM locations WHERE locationname = ? AND distance = ? LIMIT 1");
            locStmt.setString(1, locName);
            locStmt.setInt(2, locDistance);
            ResultSet rs = locStmt.executeQuery();
    
            if (!rs.next()) return "Invalid location: name or distance does not exist";
            int locId = rs.getInt("locationid");
    
            // 2. Get next_loc_id
            PreparedStatement nextStmt = centralConn.prepareStatement(
                "SELECT locationid FROM locations WHERE distance > ? ORDER BY distance ASC LIMIT 1");
            nextStmt.setInt(1, locDistance);
            ResultSet nextRs = nextStmt.executeQuery();
            Integer nextLocId = nextRs.next() ? nextRs.getInt("locationid") : null;
    
            // 3. Get prev_loc_id
            PreparedStatement prevStmt = centralConn.prepareStatement(
                "SELECT locationid FROM locations WHERE distance < ? ORDER BY distance DESC LIMIT 1");
            prevStmt.setInt(1, locDistance);
            ResultSet prevRs = prevStmt.executeQuery();
            Integer prevLocId = prevRs.next() ? prevRs.getInt("locationid") : null;
    
            if (nextLocId == null && prevLocId == null)
                return "No adjacent location available to move cabs";
    
            int targetLocId = (nextLocId != null) ? nextLocId : prevLocId;
            String moveMessage = (nextLocId != null) ? "next" : "previous";
    
            // 4. Move cabs in all shards
            for (int i = 0; i < shards; i++) {
                Connection shardConn = DatabaseConnection.getCabShardConnection(i);
                PreparedStatement updateStmt = shardConn.prepareStatement(
                    "UPDATE cabpositions SET locationid = ? WHERE locationid = ?");
                updateStmt.setInt(1, targetLocId);
                updateStmt.setInt(2, locId);
                updateStmt.executeUpdate();
                updateStmt.close();
                shardConn.close();
            }
    
            // 5. Delete the location
            PreparedStatement delStmt = centralConn.prepareStatement("DELETE FROM locations WHERE locationid = ?");
            delStmt.setInt(1, locId);
            delStmt.executeUpdate();
    
            statusMessage = "Cabs moved to the " + moveMessage + " location successfully";
    
        } catch (SQLException e) {
            e.printStackTrace();
            statusMessage = "Error occurred during location removal";
        }
    
        return statusMessage;
    }
    

    public List<CabPositions> checkAvailableCab() {
        // Step 1: Load all locations into a Map
        Map<Integer, String> locationMap = new HashMap<>();
        String locationQuery = "SELECT locationid, locationname FROM locations";

        try (Connection locationConn = DatabaseConnection.getLocationConnection();
            PreparedStatement stmt = locationConn.prepareStatement(locationQuery);
            ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                locationMap.put(rs.getInt("locationid"), rs.getString("locationname"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return Collections.emptyList();
        }

        // Step 2: Query all shards for available cab positions
        Map<Integer, List<Integer>> groupedCabs = new HashMap<>();
        String cabQuery = "SELECT locationid, cabid FROM cabpositions WHERE cabstatus = 'AVAILABLE'";

        try {
            for (Connection shardConn : DatabaseConnection.getAllCabShardConnections()) {
                try (PreparedStatement stmt = shardConn.prepareStatement(cabQuery);
                    ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        int locationId = rs.getInt("locationid");
                        int cabId = rs.getInt("cabid");
                        groupedCabs.computeIfAbsent(locationId, k -> new ArrayList<>()).add(cabId);
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        if (shardConn != null && !shardConn.isClosed()) shardConn.close();
                    } catch (SQLException ex) {
                        ex.printStackTrace();
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Step 3: Map to CabPositions
        List<CabPositions> availableCabs = new ArrayList<>();
        for (Map.Entry<Integer, List<Integer>> entry : groupedCabs.entrySet()) {
            int locationId = entry.getKey();
            String locationName = locationMap.get(locationId);
            List<Integer> cabIds = entry.getValue();
            Collections.sort(cabIds);
            String cabIdList = cabIds.stream().map(String::valueOf).collect(Collectors.joining(","));
            availableCabs.add(new CabPositions(locationName, cabIdList));
        }

        return availableCabs;
    }


    public CustomerAck getFreeCab(int customerid, String source, String destination, String cabtype, LocalDateTime customerdeparturetime, LocalDateTime customerarrivaltime) {
        int sourceDistance = 0, destinationDistance = 0;
    
        // Step 1: Get source and destination distances
        try (Connection locConn = DatabaseConnection.getLocationConnection()) {
            String query = "SELECT locationname, distance FROM locations WHERE locationname IN (?, ?)";
            try (PreparedStatement stmt = locConn.prepareStatement(query)) {
                stmt.setString(1, source);
                stmt.setString(2, destination);
                ResultSet rs = stmt.executeQuery();
                while (rs.next()) {
                    if (rs.getString("locationname").equals(source)) {
                        sourceDistance = rs.getInt("distance");
                    } else {
                        destinationDistance = rs.getInt("distance");
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    
        int bestDistance = Integer.MAX_VALUE;
        int selectedCabId = -1;
        int selectedShard = -1;
    
        // Step 2: Check each cabpositions shard
        for (int shard = 0; shard < 3; shard++) {
            try (Connection cabConn = DatabaseConnection.getCabShardConnection(shard)) {
                String cabQuery = "SELECT cabid, locationid FROM cabpositions WHERE cabstatus = 'AVAILABLE' AND cabtype = ?";
                try (PreparedStatement stmt = cabConn.prepareStatement(cabQuery)) {
                    stmt.setString(1, cabtype);
                    ResultSet rs = stmt.executeQuery();
                    while (rs.next()) {
                        int cabid = rs.getInt("cabid");
                        int locationid = rs.getInt("locationid");
    
                        // Step 3: Check online status by broadcasting to all users shards
                        boolean isOnline = false;
                        for (int userShard = 0; userShard < 3 && !isOnline; userShard++) {
                            try (Connection userConn = DatabaseConnection.getUserShardConnection(userShard)) {
                                String userQuery = "SELECT onlinestatus FROM users WHERE userid = ?";
                                try (PreparedStatement userStmt = userConn.prepareStatement(userQuery)) {
                                    userStmt.setInt(1, cabid);
                                    ResultSet userRs = userStmt.executeQuery();
                                    if (userRs.next()) {
                                        isOnline = userRs.getBoolean("onlinestatus");
                                    }
                                }
                            }
                        }
    
                        if (!isOnline) continue;
    
                        // Step 4: Check if cab is already booked (broadcast to all ride shards)
                        boolean isBooked = false;
                        for (int rideShard = 0; rideShard < 3 && !isBooked; rideShard++) {
                            try (Connection rideConn = DatabaseConnection.getRideShardConnection(rideShard)) {
                                String rideQuery = "SELECT 1 FROM ridedetails WHERE cabid = ? AND (departuretime < ? AND arrivaltime > ?) LIMIT 1";
                                try (PreparedStatement rideStmt = rideConn.prepareStatement(rideQuery)) {
                                    rideStmt.setInt(1, cabid);
                                    rideStmt.setTimestamp(2, Timestamp.valueOf(customerarrivaltime));
                                    rideStmt.setTimestamp(3, Timestamp.valueOf(customerdeparturetime));
                                    ResultSet rideRs = rideStmt.executeQuery();
                                    if (rideRs.next()) {
                                        isBooked = true;
                                    }
                                }
                            }
                        }
    
                        if (isBooked) continue;
    
                        // Step 5: Calculate distance
                        int locDistance = 0;
                        try (Connection locConn = DatabaseConnection.getLocationConnection()) {
                            String distQuery = "SELECT distance FROM locations WHERE locationid = ?";
                            try (PreparedStatement distStmt = locConn.prepareStatement(distQuery)) {
                                distStmt.setInt(1, locationid);
                                ResultSet distRs = distStmt.executeQuery();
                                if (distRs.next()) {
                                    locDistance = distRs.getInt("distance");
                                }
                            }
                        }
    
                        int nearest = Math.abs(locDistance - sourceDistance);
                        if (nearest < bestDistance) {
                            bestDistance = nearest;
                            selectedCabId = cabid;
                            selectedShard = shard;
                        }
        
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    
        // Step 6: If cab found, update status to WAIT
        if (selectedCabId != -1) {
            try (Connection updateConn = DatabaseConnection.getCabShardConnection(selectedShard)) {
                updateConn.setAutoCommit(false);
                String updateQuery = "UPDATE cabpositions SET cabstatus = 'WAIT' WHERE cabid = ?";
                try (PreparedStatement updateStmt = updateConn.prepareStatement(updateQuery)) {
                    updateStmt.setInt(1, selectedCabId);
                    updateStmt.executeUpdate();
                    updateConn.commit();
    
                    // Schedule timeout release
                    scheduleAutoRelease(selectedCabId, customerid);
    
                    // return new CustomerAck(selectedCabId, bestDistance, bestDistance * 10, source, destination);
                    int rideDistance = Math.abs(destinationDistance - sourceDistance);
                    int fare = rideDistance * 10; // assuming 10 units per distance
                    return new CustomerAck(selectedCabId, rideDistance, fare, source, destination);
                } catch (SQLException e) {
                    updateConn.rollback();
                    e.printStackTrace();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    
        return null;
    }
    
    
    // Schedules auto-release of cab after timeout, with cabpositions sharded
    private void scheduleAutoRelease(int cabId, int customerId) {
        Runnable autoReleaseTask = () -> {
            int cabshardIndex = cabId % shards; // Determine correct shard for cabId
            int customershardIndex = customerId % shards;

            try (
                Connection cabShardConnection = DatabaseConnection.getCabShardConnection(cabshardIndex); // Cab shard
                Connection customerShardConnection = DatabaseConnection.getCustomerShardConnection(customershardIndex) 
            ) {
                // 1. Update cab status if still in WAIT
                String updateQuery = "UPDATE cabpositions SET cabstatus = 'AVAILABLE' WHERE cabid = ? AND cabstatus = 'WAIT'";
                try (PreparedStatement updateStmt = cabShardConnection.prepareStatement(updateQuery)) {
                    updateStmt.setInt(1, cabId);
                    int rowsUpdated = updateStmt.executeUpdate();

                    if (rowsUpdated > 0) {
                        System.out.println("Cab " + cabId + " has been automatically released.");

                        // Notify cab driver via WebSocket
                        DriverSocket.sendCloseRequest(String.valueOf(cabId));

                        // 2. Insert penalty into centralized customerdetails (or customer_penalty table)
                        String insertPenaltyQuery = "INSERT INTO customerdetails (customerid, penalty, date) VALUES (?, ?, ?)";
                        try (PreparedStatement penaltyStmt = customerShardConnection.prepareStatement(insertPenaltyQuery)) {
                            penaltyStmt.setInt(1, customerId);
                            penaltyStmt.setInt(2, 20);
                            penaltyStmt.setDate(3, java.sql.Date.valueOf(LocalDate.now()));
                            penaltyStmt.executeUpdate();
                        }
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                scheduledTasks.remove(cabId); // Always clean up task
            }
        };

        // Schedule task after 2 minutes
        ScheduledFuture<?> future = scheduler.schedule(autoReleaseTask, 2, TimeUnit.MINUTES);
        scheduledTasks.put(cabId, future);
    }

    

    public boolean addRideHistory(int customerid, int cabid, int distance, String source, String destination, LocalDateTime departuretime, LocalDateTime arrivaltime) {
        String rideInsertQuery = "INSERT INTO ridedetails (rideid, customerid, cabid, source, destination, fare, commission, departuretime, arrivaltime) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        String updateCabQuery = "UPDATE cabpositions SET cabstatus = 'AVAILABLE' WHERE cabid = ? AND cabstatus = 'WAIT'";
    
        rideid++; // You may need a better distributed ID generation in production
    
        int rideShard = rideid % shards;
        int cabShard = cabid % shards;
    
        boolean rideInserted = false;
    
        // 1. Insert into ridedetails
        try (
            Connection rideConn = DatabaseConnection.getRideShardConnection(rideShard);
            PreparedStatement rideStmt = rideConn.prepareStatement(rideInsertQuery)
        ) {
            rideStmt.setInt(1, rideid);
            rideStmt.setInt(2, customerid);
            rideStmt.setInt(3, cabid);
            rideStmt.setString(4, source);
            rideStmt.setString(5, destination);
            rideStmt.setInt(6, distance * 10); // fare
            rideStmt.setInt(7, distance * 3);  // commission
            rideStmt.setTimestamp(8, Timestamp.valueOf(departuretime));
            rideStmt.setTimestamp(9, Timestamp.valueOf(arrivaltime));
    
            int result = rideStmt.executeUpdate();
            rideInserted = result > 0;
    
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    
        if (rideInserted) {
            // 2. Update cabpositions
            try (
                Connection cabConn = DatabaseConnection.getCabShardConnection(cabShard);
                PreparedStatement updateStmt = cabConn.prepareStatement(updateCabQuery)
            ) {
                updateStmt.setInt(1, cabid);
                updateStmt.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
                // Optional: log or schedule retry of cab status update
            }
    
            // 3. Cancel any scheduled WAIT task
            ScheduledFuture<?> future = scheduledTasks.remove(cabid);
            if (future != null) {
                future.cancel(false);
            }
    
            return true;
        }
    
        return false;
    }
    

    public boolean cancelRide(int cabid, int customerid) {
        String updateCabPositionQuery = "UPDATE cabpositions SET cabstatus = 'AVAILABLE' WHERE cabid = ? AND cabstatus = 'WAIT';";
        String insertPenaltyQuery = "INSERT INTO customerdetails (customerid, penalty, date) VALUES (?, ?, ?);";

        int cabShard = cabid % shards;
        int customerShard = customerid % shards;

        boolean updated = false;

        try (
            Connection cabConn = DatabaseConnection.getCabShardConnection(cabShard);
            PreparedStatement updateStmt = cabConn.prepareStatement(updateCabPositionQuery)
        ) {
            updateStmt.setInt(1, cabid);
            int updateResult = updateStmt.executeUpdate();
            if (updateResult > 0) {
                updated = true;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }

        if (updated) {
            try (
                Connection customerConn = DatabaseConnection.getCustomerShardConnection(customerShard); // or getCustomerShardConnection()
                PreparedStatement penaltyStmt = customerConn.prepareStatement(insertPenaltyQuery)
            ) {
                penaltyStmt.setInt(1, customerid);
                penaltyStmt.setInt(2, 20);
                penaltyStmt.setDate(3, java.sql.Date.valueOf(LocalDate.now()));

                int penaltyInserted = penaltyStmt.executeUpdate();

                // Cancel scheduled auto-release if any
                ScheduledFuture<?> future = scheduledTasks.remove(cabid);
                if (future != null) {
                    future.cancel(false);
                }

                return penaltyInserted > 0;
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return false;
    }


    public boolean updateCabPositions(int cabid, int locationid) {

        String query = "UPDATE cabpositions SET locationid = ? WHERE cabid = ?";

        try (Connection connection = DatabaseConnection.getCabShardConnection(cabid % shards);
        PreparedStatement preparedStatement = connection.prepareStatement(query)) {

       preparedStatement.setInt(1, locationid);
       preparedStatement.setInt(2, cabid);

       int val = preparedStatement.executeUpdate();

       return val > 0;

        } catch (SQLException e) {
            e.printStackTrace(); 
        }

        return false;
    }

    public List<Ride> getCustomerRideSummary(int customerid) {
        String query = "SELECT source, destination, cabid, fare FROM ridedetails WHERE customerid = ?";
        List<Ride> rides = new ArrayList<>();
    
        int shards = 3; // or use a constant from config
    
        for (int shard = 0; shard < shards; shard++) {
            try (Connection connection = DatabaseConnection.getRideShardConnection(shard);
                 PreparedStatement preparedStatement = connection.prepareStatement(query)) {
    
                preparedStatement.setInt(1, customerid);
                ResultSet result = preparedStatement.executeQuery();
    
                while (result.next()) {
                    Ride ride = new Ride(
                        result.getInt("cabid"),
                        result.getString("source"),
                        result.getString("destination"),
                        result.getInt("fare")
                    );
                    rides.add(ride);
                }
    
            } catch (SQLException e) {
                System.err.println("Error accessing ride shard " + shard);
                e.printStackTrace();
            }
        }
    
        return rides;
    }
    

    public List<Penalty> getPenalty(int customerid) {
        String query = "SELECT penalty, date FROM customerdetails WHERE customerid = ?";
        List<Penalty> penalties = new ArrayList<>();
    
        int shards = 3; // total number of customer shards
    
        for (int shard = 0; shard < shards; shard++) {
            try (Connection connection = DatabaseConnection.getCustomerShardConnection(shard);
                 PreparedStatement preparedStatement = connection.prepareStatement(query)) {
    
                preparedStatement.setInt(1, customerid);
                ResultSet result = preparedStatement.executeQuery();
    
                while (result.next()) {
                    Penalty penalty = new Penalty(
                        result.getInt("penalty"),
                        result.getObject("date", LocalDate.class)
                    );
                    penalties.add(penalty);
                }
    
            } catch (SQLException e) {
                System.err.println("Error accessing customer shard " + shard);
                e.printStackTrace();
            }
        }
    
        return penalties;
    }
    

    public List<Ride> getCabRideSummary(int cabid) {
        String query = "SELECT source, destination, customerid, fare, commission FROM ridedetails WHERE cabid = ?";
        List<Ride> rides = new ArrayList<>();
    
        int shards = 3; // total number of ride shards
    
        for (int shard = 0; shard < shards; shard++) {
            try (Connection connection = DatabaseConnection.getRideShardConnection(shard);
                 PreparedStatement preparedStatement = connection.prepareStatement(query)) {
    
                preparedStatement.setInt(1, cabid);
                ResultSet result = preparedStatement.executeQuery();
    
                while (result.next()) {
                    Ride ride = new Ride(
                        result.getInt("customerid"),
                        result.getString("source"),
                        result.getString("destination"),
                        result.getInt("fare"),
                        result.getInt("commission")
                    );
                    rides.add(ride);
                }
    
            } catch (SQLException e) {
                System.err.println("Error accessing ride shard " + shard);
                e.printStackTrace();
            }
        }
    
        return rides;
    }
    
    public List<List<Ride>> getAllCabRides() {
        String query = "SELECT cabid, customerid, source, destination, fare, commission FROM ridedetails ORDER BY cabid ASC";
        Map<Integer, List<Ride>> cabRideMap = new TreeMap<>();
    
        for (int shard = 0; shard < shards; shard++) {
            try (Connection connection = DatabaseConnection.getRideShardConnection(shard);
                 PreparedStatement preparedStatement = connection.prepareStatement(query);
                 ResultSet result = preparedStatement.executeQuery()) {
    
                while (result.next()) {
                    int cabId = result.getInt("cabid");
                    Ride ride = new Ride(
                        result.getInt("customerid"),
                        result.getString("source"),
                        result.getString("destination"),
                        result.getInt("fare"),
                        result.getInt("commission")
                    );
    
                    cabRideMap.computeIfAbsent(cabId, k -> new ArrayList<>()).add(ride);
                }
    
            } catch (SQLException e) {
                System.err.println("Error querying ride shard " + shard);
                e.printStackTrace();
            }
        }
    
        return new ArrayList<>(cabRideMap.values());
    }
    

    public List<TotalSummary> getTotalCabSummary() {
        String query = "SELECT cabid, COUNT(*) AS total_rides, SUM(fare) AS total_fare, SUM(commission) AS total_commission " +
                       "FROM ridedetails GROUP BY cabid ORDER BY cabid ASC;";
        Map<Integer, TotalSummary> summaryMap = new TreeMap<>();
    
        for (int shard = 0; shard < shards; shard++) {
            try (Connection connection = DatabaseConnection.getRideShardConnection(shard);
                 PreparedStatement preparedStatement = connection.prepareStatement(query);
                 ResultSet result = preparedStatement.executeQuery()) {
    
                while (result.next()) {
                    int cabid = result.getInt("cabid");
                    int rides = result.getInt("total_rides");
                    int fare = result.getInt("total_fare");
                    int commission = result.getInt("total_commission");
    
                    summaryMap.merge(cabid,
                        new TotalSummary(cabid, rides, fare, commission),
                        (existing, incoming) -> new TotalSummary(
                            cabid,
                            existing.getTrips() + incoming.getTrips(),
                            existing.getFare() + incoming.getFare(),
                            existing.getCommission() + incoming.getCommission()
                        )
                    );
                }
    
            } catch (SQLException e) {
                System.err.println("Error querying ride shard " + shard);
                e.printStackTrace();
            }
        }
    
        return new ArrayList<>(summaryMap.values());
    }
    

    public List<List<Ride>> getAllCustomerRides() {
        String query = "SELECT cabid, customerid, source, destination, fare FROM ridedetails ORDER BY customerid ASC";
        Map<Integer, List<Ride>> customerRideMap = new TreeMap<>(); // TreeMap keeps keys sorted
    
        for (int shard = 0; shard < shards; shard++) {
            try (Connection connection = DatabaseConnection.getRideShardConnection(shard);
                 PreparedStatement preparedStatement = connection.prepareStatement(query);
                 ResultSet result = preparedStatement.executeQuery()) {
    
                while (result.next()) {
                    int customerId = result.getInt("customerid");
                    Ride ride = new Ride(
                        result.getInt("cabid"),
                        result.getString("source"),
                        result.getString("destination"),
                        result.getInt("fare")
                    );
    
                    customerRideMap.computeIfAbsent(customerId, k -> new ArrayList<>()).add(ride);
                }
    
            } catch (SQLException e) {
                System.err.println("Error accessing shard " + shard);
                e.printStackTrace();
            }
        }
    
        // Convert the grouped values to List<List<Ride>>
        return new ArrayList<>(customerRideMap.values());
    }
    

    public List<TotalSummary> getTotalCustomerSummary() {
        String query = "SELECT customerid, COUNT(*) AS total_rides, SUM(fare) AS total_fare " +
                       "FROM ridedetails GROUP BY customerid";
        int shards = 3; // number of ride shards
    
        Map<Integer, TotalSummary> summaryMap = new TreeMap<>();
    
        for (int i = 0; i < shards; i++) {
            try (Connection connection = DatabaseConnection.getRideShardConnection(i);
                 PreparedStatement preparedStatement = connection.prepareStatement(query);
                 ResultSet result = preparedStatement.executeQuery()) {
    
                while (result.next()) {
                    int customerId = result.getInt("customerid");
                    int rides = result.getInt("total_rides");
                    int fare = result.getInt("total_fare");
    
                    TotalSummary existing = summaryMap.get(customerId);
                    if (existing == null) {
                        summaryMap.put(customerId, new TotalSummary(customerId, rides, fare));
                    } else {
                        existing.setTrips(existing.getTrips() + rides);
                        existing.setFare(existing.getFare() + fare);
                    }
                }
    
            } catch (SQLException e) {
                System.err.println("Error accessing ride shard " + i);
                e.printStackTrace();
            }
        }
    
        return new ArrayList<>(summaryMap.values());
    }


}





// package com.example.database;

// // import java.sql.CallableStatement;
// import java.sql.Connection;
// import java.sql.PreparedStatement;
// import java.sql.ResultSet;
// import java.sql.SQLException;
// import java.sql.Timestamp;
// // import java.sql.Types;
// import java.time.LocalDate;
// // import java.time.LocalDate;
// import java.time.LocalDateTime;
// import java.util.ArrayList;
// import java.util.Collections;
// import java.util.HashMap;
// import java.util.List;
// import java.util.Map;
// import java.util.TreeMap;
// import java.util.concurrent.ConcurrentHashMap;
// import java.util.concurrent.Executors;
// import java.util.concurrent.ScheduledExecutorService;
// import java.util.concurrent.ScheduledFuture;
// import java.util.concurrent.TimeUnit;
// import java.util.stream.Collectors;

// import com.example.pojo.CabPositions;
// import com.example.pojo.CustomerAck;
// import com.example.pojo.Penalty;
// import com.example.pojo.Ride;
// import com.example.pojo.TotalSummary;
// import com.example.pojo.User;
// import com.example.util.DatabaseConnection;
// import com.example.util.Gender;
// import com.example.util.Role;
// // import com.example.websocket.DriverSocket;
// import com.example.websocket.DriverSocket;

// public class DatabaseStorage implements Storage {

//     private static int userid = 0;
//     private static int rideid = 0;

//     private static final int shards = 3;
//     private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(5);
//     private static final Map<Integer, ScheduledFuture<?>> scheduledTasks = new ConcurrentHashMap<>();

//     @Override
//     public int addUser(User user) {
//         String insertuser = "INSERT INTO users (userid, name, username, password, age, gender, role) VALUES (?,?,?,?,?,?,?)";
//         // int generatedUserId = -1;
//         userid++;
//         try (Connection connection = DatabaseConnection.getUserShardConnection(userid % shards);
//              PreparedStatement preparedStatementuser = connection.prepareStatement(insertuser, PreparedStatement.RETURN_GENERATED_KEYS)) {

//             preparedStatementuser.setInt(1, userid);
//             preparedStatementuser.setString(2, user.getName());
//             preparedStatementuser.setString(3, user.getUsername());
//             preparedStatementuser.setString(4, user.getEncryptedpassword());
//             preparedStatementuser.setLong(5, user.getAge());
//             preparedStatementuser.setString(6, user.getGender().name());
//             preparedStatementuser.setString(7, user.getRole().name()); 

//             int val = preparedStatementuser.executeUpdate();

//              if (val != 0) {
//                 return userid;
//             //     // Get the generated primary key (userid)
//             //     try (ResultSet generatedKeys = preparedStatementuser.getGeneratedKeys()) {
//             //         if (generatedKeys.next()) {
//             //             generatedUserId = generatedKeys.getInt(1); // Retrieve the generated ID
//             //         }
//             //     }
//             }

//         } catch (SQLException e) {
//             e.printStackTrace(); 
//         }
//         return -1;
//     }

//     @Override
//     public User getUser(String username) {
//         String query = "SELECT * FROM users WHERE username = ?";

//         try {
//             for (Connection connection : DatabaseConnection.getAllUserShardConnections()) {
//                 try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {

//                     preparedStatement.setString(1, username);
//                     ResultSet result = preparedStatement.executeQuery();

//                     if (result.next()) {
//                         return new User(
//                             result.getInt("userid"),
//                             result.getString("name"),
//                             result.getString("username"),
//                             result.getString("password"),
//                             result.getInt("age"),
//                             Gender.valueOf(result.getString("gender")),
//                             Role.valueOf(result.getString("role"))
//                         );
//                     }

//                 } catch (SQLException e) {
//                     e.printStackTrace(); // You could log and continue
//                 } finally {
//                     try {
//                         if (connection != null && !connection.isClosed()) connection.close();
//                     } catch (SQLException ex) {
//                         ex.printStackTrace();
//                     }
//                 }
//             }
//         } catch (SQLException e) {
//             e.printStackTrace();
//         }

//         return null; // No user found in any shard
//     }


//     @Override
//     public boolean login(int userid){
//         String query = "UPDATE users SET onlinestatus = ? WHERE userid = ?";

//         try (Connection connection = DatabaseConnection.getUserShardConnection(userid % shards);
//         PreparedStatement preparedStatement = connection.prepareStatement(query)) {

//        preparedStatement.setBoolean(1, true);
//        preparedStatement.setInt(2, userid);

//        int val = preparedStatement.executeUpdate();

//        return val > 0;

//         } catch (SQLException e) {
//             e.printStackTrace(); 
//         }

//         return false;
//     }

//     @Override
//     public boolean logout(int userid){
//         String query = "UPDATE users SET onlinestatus = ? WHERE userid = ?";

//         try (Connection connection = DatabaseConnection.getUserShardConnection(userid % shards);
//         PreparedStatement preparedStatement = connection.prepareStatement(query)) {

//        preparedStatement.setBoolean(1, false);
//        preparedStatement.setInt(2, userid);

//        int val = preparedStatement.executeUpdate();

//        return val > 0;

//         } catch (SQLException e) {
//             e.printStackTrace(); 
//         }

//         return false;
//     }

//     @Override
//     public boolean addCabLocation(int cabid, int  locationid, String cabtype) {

//         String cabpositionquery = "INSERT INTO cabpositions(cabid, locationid, cabtype) VALUES(?,?,?)";

//         try (Connection connection = DatabaseConnection.getCabShardConnection(cabid % shards);
//              PreparedStatement preparedStatementcabposition = connection.prepareStatement(cabpositionquery)) {

//             preparedStatementcabposition.setInt(1, cabid);
//             preparedStatementcabposition.setInt(2, locationid);
//             preparedStatementcabposition.setString(3, cabtype);
//             int val = preparedStatementcabposition.executeUpdate();
//             return val > 0;

//         } catch (SQLException e) {
//             e.printStackTrace(); 
//         }

//         return false;

//     }

//     public int checkLocation(String cablocation) {
//         String locationquery = "SELECT locationid FROM locations WHERE locationname = ?";

//         try (Connection connection = DatabaseConnection.getLocationConnection();
//         PreparedStatement preparedStatementlocation = connection.prepareStatement(locationquery)) {

//        preparedStatementlocation.setString(1, cablocation);
//        ResultSet result = preparedStatementlocation.executeQuery();

//        if (result.next()) {
//            return result.getInt("locationid");
//        }

//         } catch (SQLException e) {
//             e.printStackTrace(); 
//         }

//         return 0;
//     }

//     public int addLocation(String locationname, int distance) {
//         String query = "INSERT INTO locations(locationname, distance) VALUES (?,?)";
//         int generatedUserId = -1;

//         try (Connection connection = DatabaseConnection.getLocationConnection();
//         PreparedStatement preparedStatement = connection.prepareStatement(query,  PreparedStatement.RETURN_GENERATED_KEYS)) {

//        preparedStatement.setString(1, locationname);
//        preparedStatement.setInt(2, distance);

//        int val = preparedStatement.executeUpdate();

//             if (val != 0) {
//                 // Get the generated primary key (userid)
//                 try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) {
//                     if (generatedKeys.next()) {
//                         generatedUserId = generatedKeys.getInt(1); // Retrieve the generated ID
//                     }
//                 }
//             }
//             return generatedUserId;

//         } catch (SQLException e) {
//             e.printStackTrace(); 
//         }

//         return -1;
//     }

//     public String removeLocation(String locName, int locDistance) {
//         String statusMessage;
//         int shards = 3;
    
//         try ( Connection centralConn = DatabaseConnection.getLocationConnection();){
//             // 1. Get loc_id
//             PreparedStatement locStmt = centralConn.prepareStatement(
//                 "SELECT locationid FROM locations WHERE locationname = ? AND distance = ? LIMIT 1");
//             locStmt.setString(1, locName);
//             locStmt.setInt(2, locDistance);
//             ResultSet rs = locStmt.executeQuery();
    
//             if (!rs.next()) return "Invalid location: name or distance does not exist";
//             int locId = rs.getInt("locationid");
    
//             // 2. Get next_loc_id
//             PreparedStatement nextStmt = centralConn.prepareStatement(
//                 "SELECT locationid FROM locations WHERE distance > ? ORDER BY distance ASC LIMIT 1");
//             nextStmt.setInt(1, locDistance);
//             ResultSet nextRs = nextStmt.executeQuery();
//             Integer nextLocId = nextRs.next() ? nextRs.getInt("locationid") : null;
    
//             // 3. Get prev_loc_id
//             PreparedStatement prevStmt = centralConn.prepareStatement(
//                 "SELECT locationid FROM locations WHERE distance < ? ORDER BY distance DESC LIMIT 1");
//             prevStmt.setInt(1, locDistance);
//             ResultSet prevRs = prevStmt.executeQuery();
//             Integer prevLocId = prevRs.next() ? prevRs.getInt("locationid") : null;
    
//             if (nextLocId == null && prevLocId == null)
//                 return "No adjacent location available to move cabs";
    
//             int targetLocId = (nextLocId != null) ? nextLocId : prevLocId;
//             String moveMessage = (nextLocId != null) ? "next" : "previous";
    
//             // 4. Move cabs in all shards
//             for (int i = 0; i < shards; i++) {
//                 Connection shardConn = DatabaseConnection.getCabShardConnection(i);
//                 PreparedStatement updateStmt = shardConn.prepareStatement(
//                     "UPDATE cabpositions SET locationid = ? WHERE locationid = ?");
//                 updateStmt.setInt(1, targetLocId);
//                 updateStmt.setInt(2, locId);
//                 updateStmt.executeUpdate();
//                 updateStmt.close();
//                 shardConn.close();
//             }
    
//             // 5. Delete the location
//             PreparedStatement delStmt = centralConn.prepareStatement("DELETE FROM locations WHERE locationid = ?");
//             delStmt.setInt(1, locId);
//             delStmt.executeUpdate();
    
//             statusMessage = "Cabs moved to the " + moveMessage + " location successfully";
    
//         } catch (SQLException e) {
//             e.printStackTrace();
//             statusMessage = "Error occurred during location removal";
//         }
    
//         return statusMessage;
//     }
    

//     public List<CabPositions> checkAvailableCab() {
//         // Step 1: Load all locations into a Map
//         Map<Integer, String> locationMap = new HashMap<>();
//         String locationQuery = "SELECT locationid, locationname FROM locations";

//         try (Connection locationConn = DatabaseConnection.getLocationConnection();
//             PreparedStatement stmt = locationConn.prepareStatement(locationQuery);
//             ResultSet rs = stmt.executeQuery()) {
//             while (rs.next()) {
//                 locationMap.put(rs.getInt("locationid"), rs.getString("locationname"));
//             }
//         } catch (SQLException e) {
//             e.printStackTrace();
//             return Collections.emptyList();
//         }

//         // Step 2: Query all shards for available cab positions
//         Map<Integer, List<Integer>> groupedCabs = new HashMap<>();
//         String cabQuery = "SELECT locationid, cabid FROM cabpositions WHERE cabstatus = 'AVAILABLE'";

//         try {
//             for (Connection shardConn : DatabaseConnection.getAllCabShardConnections()) {
//                 try (PreparedStatement stmt = shardConn.prepareStatement(cabQuery);
//                     ResultSet rs = stmt.executeQuery()) {
//                     while (rs.next()) {
//                         int locationId = rs.getInt("locationid");
//                         int cabId = rs.getInt("cabid");
//                         groupedCabs.computeIfAbsent(locationId, k -> new ArrayList<>()).add(cabId);
//                     }
//                 } catch (SQLException e) {
//                     e.printStackTrace();
//                 } finally {
//                     try {
//                         if (shardConn != null && !shardConn.isClosed()) shardConn.close();
//                     } catch (SQLException ex) {
//                         ex.printStackTrace();
//                     }
//                 }
//             }
//         } catch (SQLException e) {
//             e.printStackTrace();
//         }

//         // Step 3: Map to CabPositions
//         List<CabPositions> availableCabs = new ArrayList<>();
//         for (Map.Entry<Integer, List<Integer>> entry : groupedCabs.entrySet()) {
//             int locationId = entry.getKey();
//             String locationName = locationMap.get(locationId);
//             List<Integer> cabIds = entry.getValue();
//             Collections.sort(cabIds);
//             String cabIdList = cabIds.stream().map(String::valueOf).collect(Collectors.joining(","));
//             availableCabs.add(new CabPositions(locationName, cabIdList));
//         }

//         return availableCabs;
//     }


//     public CustomerAck getFreeCab(int customerid, String source, String destination, String cabtype, LocalDateTime customerdeparturetime, LocalDateTime customerarrivaltime) {
//         int sourceDistance = 0, destinationDistance = 0;
    
//         // Step 1: Get source and destination distances
//         try (Connection locConn = DatabaseConnection.getLocationConnection()) {
//             String query = "SELECT locationname, distance FROM locations WHERE locationname IN (?, ?)";
//             try (PreparedStatement stmt = locConn.prepareStatement(query)) {
//                 stmt.setString(1, source);
//                 stmt.setString(2, destination);
//                 ResultSet rs = stmt.executeQuery();
//                 while (rs.next()) {
//                     if (rs.getString("locationname").equals(source)) {
//                         sourceDistance = rs.getInt("distance");
//                     } else {
//                         destinationDistance = rs.getInt("distance");
//                     }
//                 }
//             }
//         } catch (SQLException e) {
//             e.printStackTrace();
//             return null;
//         }
    
//         int bestDistance = Integer.MAX_VALUE;
//         int selectedCabId = -1;
//         int selectedShard = -1;
    
//         // Step 2: Check each cabpositions shard
//         for (int shard = 0; shard < 3; shard++) {
//             try (Connection cabConn = DatabaseConnection.getCabShardConnection(shard)) {
//                 String cabQuery = "SELECT cabid, locationid FROM cabpositions WHERE cabstatus = 'AVAILABLE' AND cabtype = ?";
//                 try (PreparedStatement stmt = cabConn.prepareStatement(cabQuery)) {
//                     stmt.setString(1, cabtype);
//                     ResultSet rs = stmt.executeQuery();
//                     while (rs.next()) {
//                         int cabid = rs.getInt("cabid");
//                         int locationid = rs.getInt("locationid");
    
//                         // Step 3: Check online status by broadcasting to all users shards
//                         boolean isOnline = false;
//                         for (int userShard = 0; userShard < 3 && !isOnline; userShard++) {
//                             try (Connection userConn = DatabaseConnection.getUserShardConnection(userShard)) {
//                                 String userQuery = "SELECT onlinestatus FROM users WHERE userid = ?";
//                                 try (PreparedStatement userStmt = userConn.prepareStatement(userQuery)) {
//                                     userStmt.setInt(1, cabid);
//                                     ResultSet userRs = userStmt.executeQuery();
//                                     if (userRs.next()) {
//                                         isOnline = userRs.getBoolean("onlinestatus");
//                                     }
//                                 }
//                             }
//                         }
    
//                         if (!isOnline) continue;
    
//                         // Step 4: Check if cab is already booked (broadcast to all ride shards)
//                         boolean isBooked = false;
//                         for (int rideShard = 0; rideShard < 3 && !isBooked; rideShard++) {
//                             try (Connection rideConn = DatabaseConnection.getRideShardConnection(rideShard)) {
//                                 String rideQuery = "SELECT 1 FROM ridedetails WHERE cabid = ? AND (departuretime < ? AND arrivaltime > ?) LIMIT 1";
//                                 try (PreparedStatement rideStmt = rideConn.prepareStatement(rideQuery)) {
//                                     rideStmt.setInt(1, cabid);
//                                     rideStmt.setTimestamp(2, Timestamp.valueOf(customerarrivaltime));
//                                     rideStmt.setTimestamp(3, Timestamp.valueOf(customerdeparturetime));
//                                     ResultSet rideRs = rideStmt.executeQuery();
//                                     if (rideRs.next()) {
//                                         isBooked = true;
//                                     }
//                                 }
//                             }
//                         }
    
//                         if (isBooked) continue;
    
//                         // Step 5: Calculate distance
//                         int locDistance = 0;
//                         try (Connection locConn = DatabaseConnection.getLocationConnection()) {
//                             String distQuery = "SELECT distance FROM locations WHERE locationid = ?";
//                             try (PreparedStatement distStmt = locConn.prepareStatement(distQuery)) {
//                                 distStmt.setInt(1, locationid);
//                                 ResultSet distRs = distStmt.executeQuery();
//                                 if (distRs.next()) {
//                                     locDistance = distRs.getInt("distance");
//                                 }
//                             }
//                         }
    
//                         int distance = Math.abs(locDistance - sourceDistance);
//                         if (distance < bestDistance) {
//                             bestDistance = distance;
//                             selectedCabId = cabid;
//                             selectedShard = shard;
//                         }
//                     }
//                 }
//             } catch (SQLException e) {
//                 e.printStackTrace();
//             }
//         }
    
//         // Step 6: If cab found, update status to WAIT
//         if (selectedCabId != -1) {
//             try (Connection updateConn = DatabaseConnection.getCabShardConnection(selectedShard)) {
//                 updateConn.setAutoCommit(false);
//                 String updateQuery = "UPDATE cabpositions SET cabstatus = 'WAIT' WHERE cabid = ?";
//                 try (PreparedStatement updateStmt = updateConn.prepareStatement(updateQuery)) {
//                     updateStmt.setInt(1, selectedCabId);
//                     updateStmt.executeUpdate();
//                     updateConn.commit();
    
//                     // Schedule timeout release
//                     scheduleAutoRelease(selectedCabId, customerid);
    
//                     return new CustomerAck(selectedCabId, bestDistance, bestDistance * 10, source, destination);
//                 } catch (SQLException e) {
//                     updateConn.rollback();
//                     e.printStackTrace();
//                 }
//             } catch (SQLException e) {
//                 e.printStackTrace();
//             }
//         }
    
//         return null;
//     }
    
    
//     // Schedules auto-release of cab after timeout, with cabpositions sharded
//     private void scheduleAutoRelease(int cabId, int customerId) {
//         Runnable autoReleaseTask = () -> {
//             int cabshardIndex = cabId % shards; // Determine correct shard for cabId
//             int customershardIndex = customerId % shards;

//             try (
//                 Connection cabShardConnection = DatabaseConnection.getCabShardConnection(cabshardIndex); // Cab shard
//                 Connection customerShardConnection = DatabaseConnection.getCustomerShardConnection(customershardIndex) // Central DB for penalty
//             ) {
//                 // 1. Update cab status if still in WAIT
//                 String updateQuery = "UPDATE cabpositions SET cabstatus = 'AVAILABLE' WHERE cabid = ? AND cabstatus = 'WAIT'";
//                 try (PreparedStatement updateStmt = cabShardConnection.prepareStatement(updateQuery)) {
//                     updateStmt.setInt(1, cabId);
//                     int rowsUpdated = updateStmt.executeUpdate();

//                     if (rowsUpdated > 0) {
//                         System.out.println("Cab " + cabId + " has been automatically released.");

//                         // Notify cab driver via WebSocket
//                         DriverSocket.sendCloseRequest(String.valueOf(cabId));

//                         // 2. Insert penalty into centralized customerdetails (or customer_penalty table)
//                         String insertPenaltyQuery = "INSERT INTO customerdetails (customerid, penalty, date) VALUES (?, ?, ?)";
//                         try (PreparedStatement penaltyStmt = customerShardConnection.prepareStatement(insertPenaltyQuery)) {
//                             penaltyStmt.setInt(1, customerId);
//                             penaltyStmt.setInt(2, 20);
//                             penaltyStmt.setDate(3, java.sql.Date.valueOf(LocalDate.now()));
//                             penaltyStmt.executeUpdate();
//                         }
//                     }
//                 }
//             } catch (SQLException e) {
//                 e.printStackTrace();
//             } finally {
//                 scheduledTasks.remove(cabId); // Always clean up task
//             }
//         };

//         // Schedule task after 2 minutes
//         ScheduledFuture<?> future = scheduler.schedule(autoReleaseTask, 2, TimeUnit.MINUTES);
//         scheduledTasks.put(cabId, future);
//     }

    

//     public boolean addRideHistory(int customerid, int cabid, int distance, String source, String destination, LocalDateTime departuretime, LocalDateTime arrivaltime) {
//         String rideInsertQuery = "INSERT INTO ridedetails (rideid, customerid, cabid, source, destination, fare, commission, departuretime, arrivaltime) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
//         String updateCabQuery = "UPDATE cabpositions SET cabstatus = 'AVAILABLE' WHERE cabid = ? AND cabstatus = 'WAIT'";
    
//         rideid++; // You may need a better distributed ID generation in production
    
//         int rideShard = rideid % shards;
//         int cabShard = cabid % shards;
    
//         boolean rideInserted = false;
    
//         // 1. Insert into ridedetails
//         try (
//             Connection rideConn = DatabaseConnection.getRideShardConnection(rideShard);
//             PreparedStatement rideStmt = rideConn.prepareStatement(rideInsertQuery)
//         ) {
//             rideStmt.setInt(1, rideid);
//             rideStmt.setInt(2, customerid);
//             rideStmt.setInt(3, cabid);
//             rideStmt.setString(4, source);
//             rideStmt.setString(5, destination);
//             rideStmt.setInt(6, distance * 10); // fare
//             rideStmt.setInt(7, distance * 3);  // commission
//             rideStmt.setTimestamp(8, Timestamp.valueOf(departuretime));
//             rideStmt.setTimestamp(9, Timestamp.valueOf(arrivaltime));
    
//             int result = rideStmt.executeUpdate();
//             rideInserted = result > 0;
    
//         } catch (SQLException e) {
//             e.printStackTrace();
//             return false;
//         }
    
//         if (rideInserted) {
//             // 2. Update cabpositions
//             try (
//                 Connection cabConn = DatabaseConnection.getCabShardConnection(cabShard);
//                 PreparedStatement updateStmt = cabConn.prepareStatement(updateCabQuery)
//             ) {
//                 updateStmt.setInt(1, cabid);
//                 updateStmt.executeUpdate();
//             } catch (SQLException e) {
//                 e.printStackTrace();
//                 // Optional: log or schedule retry of cab status update
//             }
    
//             // 3. Cancel any scheduled WAIT task
//             ScheduledFuture<?> future = scheduledTasks.remove(cabid);
//             if (future != null) {
//                 future.cancel(false);
//             }
    
//             return true;
//         }
    
//         return false;
//     }
    

//     public boolean cancelRide(int cabid, int customerid) {
//         String updateCabPositionQuery = "UPDATE cabpositions SET cabstatus = 'AVAILABLE' WHERE cabid = ? AND cabstatus = 'WAIT';";
//         String insertPenaltyQuery = "INSERT INTO customerdetails (customerid, penalty, date) VALUES (?, ?, ?);";

//         int cabShard = cabid % shards;
//         int customerShard = customerid % shards;

//         boolean updated = false;

//         try (
//             Connection cabConn = DatabaseConnection.getCabShardConnection(cabShard);
//             PreparedStatement updateStmt = cabConn.prepareStatement(updateCabPositionQuery)
//         ) {
//             updateStmt.setInt(1, cabid);
//             int updateResult = updateStmt.executeUpdate();
//             if (updateResult > 0) {
//                 updated = true;
//             }
//         } catch (SQLException e) {
//             e.printStackTrace();
//             return false;
//         }

//         if (updated) {
//             try (
//                 Connection customerConn = DatabaseConnection.getCustomerShardConnection(customerShard); // or getCustomerShardConnection()
//                 PreparedStatement penaltyStmt = customerConn.prepareStatement(insertPenaltyQuery)
//             ) {
//                 penaltyStmt.setInt(1, customerid);
//                 penaltyStmt.setInt(2, 20);
//                 penaltyStmt.setDate(3, java.sql.Date.valueOf(LocalDate.now()));

//                 int penaltyInserted = penaltyStmt.executeUpdate();

//                 // Cancel scheduled auto-release if any
//                 ScheduledFuture<?> future = scheduledTasks.remove(cabid);
//                 if (future != null) {
//                     future.cancel(false);
//                 }

//                 return penaltyInserted > 0;
//             } catch (SQLException e) {
//                 e.printStackTrace();
//             }
//         }

//         return false;
//     }


//     public boolean updateCabPositions(int cabid, int locationid) {

//         String query = "UPDATE cabpositions SET locationid = ? WHERE cabid = ?";

//         try (Connection connection = DatabaseConnection.getCabShardConnection(cabid % shards);
//         PreparedStatement preparedStatement = connection.prepareStatement(query)) {

//        preparedStatement.setInt(1, locationid);
//        preparedStatement.setInt(2, cabid);

//        int val = preparedStatement.executeUpdate();

//        return val > 0;

//         } catch (SQLException e) {
//             e.printStackTrace(); 
//         }

//         return false;
//     }

//     public List<Ride> getCustomerRideSummary(int customerid) {
//         String query = "SELECT source, destination, cabid, fare FROM ridedetails WHERE customerid = ?";
//         List<Ride> rides = new ArrayList<>();
    
//         int shards = 3; // or use a constant from config
    
//         for (int shard = 0; shard < shards; shard++) {
//             try (Connection connection = DatabaseConnection.getRideShardConnection(shard);
//                  PreparedStatement preparedStatement = connection.prepareStatement(query)) {
    
//                 preparedStatement.setInt(1, customerid);
//                 ResultSet result = preparedStatement.executeQuery();
    
//                 while (result.next()) {
//                     Ride ride = new Ride(
//                         result.getInt("cabid"),
//                         result.getString("source"),
//                         result.getString("destination"),
//                         result.getInt("fare")
//                     );
//                     rides.add(ride);
//                 }
    
//             } catch (SQLException e) {
//                 System.err.println("Error accessing ride shard " + shard);
//                 e.printStackTrace();
//             }
//         }
    
//         return rides;
//     }
    

//     public List<Penalty> getPenalty(int customerid) {
//         String query = "SELECT penalty, date FROM customerdetails WHERE customerid = ?";
//         List<Penalty> penalties = new ArrayList<>();
    
//         int shards = 3; // total number of customer shards
    
//         for (int shard = 0; shard < shards; shard++) {
//             try (Connection connection = DatabaseConnection.getCustomerShardConnection(shard);
//                  PreparedStatement preparedStatement = connection.prepareStatement(query)) {
    
//                 preparedStatement.setInt(1, customerid);
//                 ResultSet result = preparedStatement.executeQuery();
    
//                 while (result.next()) {
//                     Penalty penalty = new Penalty(
//                         result.getInt("penalty"),
//                         result.getObject("date", LocalDate.class)
//                     );
//                     penalties.add(penalty);
//                 }
    
//             } catch (SQLException e) {
//                 System.err.println("Error accessing customer shard " + shard);
//                 e.printStackTrace();
//             }
//         }
    
//         return penalties;
//     }
    

//     public List<Ride> getCabRideSummary(int cabid) {
//         String query = "SELECT source, destination, customerid, fare, commission FROM ridedetails WHERE cabid = ?";
//         List<Ride> rides = new ArrayList<>();
    
//         int shards = 3; // total number of ride shards
    
//         for (int shard = 0; shard < shards; shard++) {
//             try (Connection connection = DatabaseConnection.getRideShardConnection(shard);
//                  PreparedStatement preparedStatement = connection.prepareStatement(query)) {
    
//                 preparedStatement.setInt(1, cabid);
//                 ResultSet result = preparedStatement.executeQuery();
    
//                 while (result.next()) {
//                     Ride ride = new Ride(
//                         result.getInt("customerid"),
//                         result.getString("source"),
//                         result.getString("destination"),
//                         result.getInt("fare"),
//                         result.getInt("commission")
//                     );
//                     rides.add(ride);
//                 }
    
//             } catch (SQLException e) {
//                 System.err.println("Error accessing ride shard " + shard);
//                 e.printStackTrace();
//             }
//         }
    
//         return rides;
//     }
    
//     public List<List<Ride>> getAllCabRides() {
//         String query = "SELECT cabid, customerid, source, destination, fare, commission FROM ridedetails ORDER BY cabid ASC";
//         Map<Integer, List<Ride>> cabRideMap = new TreeMap<>();
    
//         for (int shard = 0; shard < shards; shard++) {
//             try (Connection connection = DatabaseConnection.getRideShardConnection(shard);
//                  PreparedStatement preparedStatement = connection.prepareStatement(query);
//                  ResultSet result = preparedStatement.executeQuery()) {
    
//                 while (result.next()) {
//                     int cabId = result.getInt("cabid");
//                     Ride ride = new Ride(
//                         result.getInt("customerid"),
//                         result.getString("source"),
//                         result.getString("destination"),
//                         result.getInt("fare"),
//                         result.getInt("commission")
//                     );
    
//                     cabRideMap.computeIfAbsent(cabId, k -> new ArrayList<>()).add(ride);
//                 }
    
//             } catch (SQLException e) {
//                 System.err.println("Error querying ride shard " + shard);
//                 e.printStackTrace();
//             }
//         }
    
//         return new ArrayList<>(cabRideMap.values());
//     }
    

//     public List<TotalSummary> getTotalCabSummary() {
//         String query = "SELECT cabid, COUNT(*) AS total_rides, SUM(fare) AS total_fare, SUM(commission) AS total_commission " +
//                        "FROM ridedetails GROUP BY cabid ORDER BY cabid ASC;";
//         Map<Integer, TotalSummary> summaryMap = new TreeMap<>();
    
//         for (int shard = 0; shard < shards; shard++) {
//             try (Connection connection = DatabaseConnection.getRideShardConnection(shard);
//                  PreparedStatement preparedStatement = connection.prepareStatement(query);
//                  ResultSet result = preparedStatement.executeQuery()) {
    
//                 while (result.next()) {
//                     int cabid = result.getInt("cabid");
//                     int rides = result.getInt("total_rides");
//                     int fare = result.getInt("total_fare");
//                     int commission = result.getInt("total_commission");
    
//                     summaryMap.merge(cabid,
//                         new TotalSummary(cabid, rides, fare, commission),
//                         (existing, incoming) -> new TotalSummary(
//                             cabid,
//                             existing.getTrips() + incoming.getTrips(),
//                             existing.getFare() + incoming.getFare(),
//                             existing.getCommission() + incoming.getCommission()
//                         )
//                     );
//                 }
    
//             } catch (SQLException e) {
//                 System.err.println("Error querying ride shard " + shard);
//                 e.printStackTrace();
//             }
//         }
    
//         return new ArrayList<>(summaryMap.values());
//     }
    

//     public List<List<Ride>> getAllCustomerRides() {
//         String query = "SELECT cabid, customerid, source, destination, fare FROM ridedetails ORDER BY customerid ASC";
//         Map<Integer, List<Ride>> customerRideMap = new TreeMap<>(); // TreeMap keeps keys sorted
    
//         for (int shard = 0; shard < shards; shard++) {
//             try (Connection connection = DatabaseConnection.getRideShardConnection(shard);
//                  PreparedStatement preparedStatement = connection.prepareStatement(query);
//                  ResultSet result = preparedStatement.executeQuery()) {
    
//                 while (result.next()) {
//                     int customerId = result.getInt("customerid");
//                     Ride ride = new Ride(
//                         result.getInt("cabid"),
//                         result.getString("source"),
//                         result.getString("destination"),
//                         result.getInt("fare")
//                     );
    
//                     customerRideMap.computeIfAbsent(customerId, k -> new ArrayList<>()).add(ride);
//                 }
    
//             } catch (SQLException e) {
//                 System.err.println("Error accessing shard " + shard);
//                 e.printStackTrace();
//             }
//         }
    
//         // Convert the grouped values to List<List<Ride>>
//         return new ArrayList<>(customerRideMap.values());
//     }
    

//     public List<TotalSummary> getTotalCustomerSummary() {
//         String query = "SELECT customerid, COUNT(*) AS total_rides, SUM(fare) AS total_fare " +
//                        "FROM ridedetails GROUP BY customerid";
//         int shards = 3; // number of ride shards
    
//         Map<Integer, TotalSummary> summaryMap = new TreeMap<>();
    
//         for (int i = 0; i < shards; i++) {
//             try (Connection connection = DatabaseConnection.getRideShardConnection(i);
//                  PreparedStatement preparedStatement = connection.prepareStatement(query);
//                  ResultSet result = preparedStatement.executeQuery()) {
    
//                 while (result.next()) {
//                     int customerId = result.getInt("customerid");
//                     int rides = result.getInt("total_rides");
//                     int fare = result.getInt("total_fare");
    
//                     TotalSummary existing = summaryMap.get(customerId);
//                     if (existing == null) {
//                         summaryMap.put(customerId, new TotalSummary(customerId, rides, fare));
//                     } else {
//                         existing.setTrips(existing.getTrips() + rides);
//                         existing.setFare(existing.getFare() + fare);
//                     }
//                 }
    
//             } catch (SQLException e) {
//                 System.err.println("Error accessing ride shard " + i);
//                 e.printStackTrace();
//             }
//         }
    
//         return new ArrayList<>(summaryMap.values());
//     }


// }
