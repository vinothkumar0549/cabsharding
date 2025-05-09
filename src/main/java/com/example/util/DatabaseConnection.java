package com.example.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class DatabaseConnection {
    private static final String LOOKUP_DB_URL = "jdbc:mysql://localhost:3306/shard_lookup_db";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "mysql";

    private static ShardManager shardManager;

    static {
        try {
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            Connection lookupConnection = DriverManager.getConnection(LOOKUP_DB_URL, DB_USER, DB_PASSWORD);
            shardManager = new ShardManager(lookupConnection);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Returns connection to appropriate shard based on user ID
    public static Connection getShardConnection(int userId) throws SQLException {
        // int shardId = shardManager.getShardId(userId);
        return shardManager.getShardConnection(shardManager.getShardId(userId));
    }

    public static List<Connection> getAllUserShardConnections() throws SQLException {
        List<Connection> shardConnections = new ArrayList<>();

        try (Connection lookupConnection = DriverManager.getConnection(LOOKUP_DB_URL, DB_USER, DB_PASSWORD);
             Statement stmt = lookupConnection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT shard_id FROM shard_lookup")) {

            while (rs.next()) {
                int shardId = rs.getInt("shard_id");
                String shardDbUrl = "jdbc:mysql://localhost:3306/users" + shardId;
                Connection shardConnection = DriverManager.getConnection(shardDbUrl, DB_USER, DB_PASSWORD);
                shardConnections.add(shardConnection);
            }
        }

        return shardConnections;
    }
    
    public static Connection getLocationConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:mysql://localhost:3306/location", "root", "mysql");
    }

}






// package com.example.util;

// import java.sql.Connection;
// import java.sql.DriverManager;
// import java.sql.SQLException;

// public class DatabaseConnection {

//     // Change these as per your actual database configuration
//     private static final String LOCATION_DB_URL = "jdbc:mysql://localhost:3306/location";

//     private static final String[] CAB_SHARD_URLS = {
//         "jdbc:mysql://localhost:3306/cabpositionshard0",
//         "jdbc:mysql://localhost:3306/cabpositionshard1",
//         "jdbc:mysql://localhost:3306/cabpositionshard2"
//     };
//     private static final String[] USER_SHARD_URLS = {
//         "jdbc:mysql://localhost:3306/usershard0",
//         "jdbc:mysql://localhost:3306/usershard1",
//         "jdbc:mysql://localhost:3306/usershard2"
//     };
//     private static final String[] RIDE_SHARD_URLS = {
//         "jdbc:mysql://localhost:3306/ridedetailshard0",
//         "jdbc:mysql://localhost:3306/ridedetailshard1",
//         "jdbc:mysql://localhost:3306/ridedetailshard2"
//     };

//     private static final String[] CUSTOMER_SHARD_URLS = {
//         "jdbc:mysql://localhost:3306/customerdetailshard0",
//         "jdbc:mysql://localhost:3306/customerdetailshard1",
//         "jdbc:mysql://localhost:3306/customerdetailshard2"
//     };

//     private static final String USERNAME = "root";
//     private static final String PASSWORD = "mysql";

//     static {
//         try {
//             // Load MySQL JDBC Driver
//             Class.forName("com.mysql.cj.jdbc.Driver");
//         } catch (ClassNotFoundException e) {
//             throw new RuntimeException("Failed to load MySQL driver", e);
//         }
//     }

//     // Get connection to cabpositions shard
//     public static Connection getCabShardConnection(int shardIndex) throws SQLException {
//         return DriverManager.getConnection(CAB_SHARD_URLS[shardIndex], USERNAME, PASSWORD);
//     }

//     // Get connection to users shard
//     public static Connection getUserShardConnection(int shardIndex) throws SQLException {
//         return DriverManager.getConnection(USER_SHARD_URLS[shardIndex], USERNAME, PASSWORD);
//     }

//     // Get connection to ridedetails shard
//     public static Connection getRideShardConnection(int shardIndex) throws SQLException {
//         return DriverManager.getConnection(RIDE_SHARD_URLS[shardIndex], USERNAME, PASSWORD);
//     }

//     // Get connection to ridedetails shard
//     public static Connection getCustomerShardConnection(int shardIndex) throws SQLException {
//         return DriverManager.getConnection(CUSTOMER_SHARD_URLS[shardIndex], USERNAME, PASSWORD);
//     }

//     // Get connection to central location DB
//     public static Connection getLocationConnection() throws SQLException {
//         return DriverManager.getConnection(LOCATION_DB_URL, USERNAME, PASSWORD);
//     }

//     // Get all user shard connections
//     public static Connection[] getAllUserShardConnections() throws SQLException {
//         Connection[] connections = new Connection[USER_SHARD_URLS.length];
//         for (int i = 0; i < USER_SHARD_URLS.length; i++) {
//             connections[i] = DriverManager.getConnection(USER_SHARD_URLS[i], USERNAME, PASSWORD);
//         }
//         return connections;
//     }

//     // Get all cab shard connections
//     public static Connection[] getAllCabShardConnections() throws SQLException {
//         Connection[] connections = new Connection[CAB_SHARD_URLS.length];
//         for (int i = 0; i < CAB_SHARD_URLS.length; i++) {
//             connections[i] = DriverManager.getConnection(CAB_SHARD_URLS[i], USERNAME, PASSWORD);
//         }
//         return connections;
//     }

//     // Get all ride shard connections
//     public static Connection[] getAllRideShardConnections() throws SQLException {
//         Connection[] connections = new Connection[RIDE_SHARD_URLS.length];
//         for (int i = 0; i < RIDE_SHARD_URLS.length; i++) {
//             connections[i] = DriverManager.getConnection(RIDE_SHARD_URLS[i], USERNAME, PASSWORD);
//         }
//         return connections;
//     }

//     // Get all ride shard connections
//     public static Connection[] getAllCustomerShardConnections() throws SQLException {
//         Connection[] connections = new Connection[CUSTOMER_SHARD_URLS.length];
//         for (int i = 0; i < CUSTOMER_SHARD_URLS.length; i++) {
//             connections[i] = DriverManager.getConnection(CUSTOMER_SHARD_URLS[i], USERNAME, PASSWORD);
//         }
//         return connections;
//     }

// }