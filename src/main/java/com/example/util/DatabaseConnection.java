
package com.example.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DatabaseConnection {

    // Change these as per your actual database configuration
    private static final String LOCATION_DB_URL = "jdbc:mysql://localhost:3306/location";

    private static final String[] CAB_SHARD_URLS = {
        "jdbc:mysql://localhost:3306/cabpositionshard0",
        "jdbc:mysql://localhost:3306/cabpositionshard1",
        "jdbc:mysql://localhost:3306/cabpositionshard2"
    };
    private static final String[] USER_SHARD_URLS = {
        "jdbc:mysql://localhost:3306/usershard0",
        "jdbc:mysql://localhost:3306/usershard1",
        "jdbc:mysql://localhost:3306/usershard2"
    };
    private static final String[] RIDE_SHARD_URLS = {
        "jdbc:mysql://localhost:3306/ridedetailshard0",
        "jdbc:mysql://localhost:3306/ridedetailshard1",
        "jdbc:mysql://localhost:3306/ridedetailshard2"
    };

    private static final String[] CUSTOMER_SHARD_URLS = {
        "jdbc:mysql://localhost:3306/customerdetailshard0",
        "jdbc:mysql://localhost:3306/customerdetailshard1",
        "jdbc:mysql://localhost:3306/customerdetailshard2"
    };

    private static final String USERNAME = "root";
    private static final String PASSWORD = "mysql";

    static {
        try {
            // Load MySQL JDBC Driver
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load MySQL driver", e);
        }
    }

    // Get connection to cabpositions shard
    public static Connection getCabShardConnection(int shardIndex) throws SQLException {
        return DriverManager.getConnection(CAB_SHARD_URLS[shardIndex], USERNAME, PASSWORD);
    }

    // Get connection to users shard
    public static Connection getUserShardConnection(int shardIndex) throws SQLException {
        return DriverManager.getConnection(USER_SHARD_URLS[shardIndex], USERNAME, PASSWORD);
    }

    // Get connection to ridedetails shard
    public static Connection getRideShardConnection(int shardIndex) throws SQLException {
        return DriverManager.getConnection(RIDE_SHARD_URLS[shardIndex], USERNAME, PASSWORD);
    }

    // Get connection to ridedetails shard
    public static Connection getCustomerShardConnection(int shardIndex) throws SQLException {
        return DriverManager.getConnection(CUSTOMER_SHARD_URLS[shardIndex], USERNAME, PASSWORD);
    }

    // Get connection to central location DB
    public static Connection getLocationConnection() throws SQLException {
        return DriverManager.getConnection(LOCATION_DB_URL, USERNAME, PASSWORD);
    }

    // Get all user shard connections
    public static Connection[] getAllUserShardConnections() throws SQLException {
        Connection[] connections = new Connection[USER_SHARD_URLS.length];
        for (int i = 0; i < USER_SHARD_URLS.length; i++) {
            connections[i] = DriverManager.getConnection(USER_SHARD_URLS[i], USERNAME, PASSWORD);
        }
        return connections;
    }

    // Get all cab shard connections
    public static Connection[] getAllCabShardConnections() throws SQLException {
        Connection[] connections = new Connection[CAB_SHARD_URLS.length];
        for (int i = 0; i < CAB_SHARD_URLS.length; i++) {
            connections[i] = DriverManager.getConnection(CAB_SHARD_URLS[i], USERNAME, PASSWORD);
        }
        return connections;
    }

    // Get all ride shard connections
    public static Connection[] getAllRideShardConnections() throws SQLException {
        Connection[] connections = new Connection[RIDE_SHARD_URLS.length];
        for (int i = 0; i < RIDE_SHARD_URLS.length; i++) {
            connections[i] = DriverManager.getConnection(RIDE_SHARD_URLS[i], USERNAME, PASSWORD);
        }
        return connections;
    }

    // Get all ride shard connections
    public static Connection[] getAllCustomerShardConnections() throws SQLException {
        Connection[] connections = new Connection[CUSTOMER_SHARD_URLS.length];
        for (int i = 0; i < CUSTOMER_SHARD_URLS.length; i++) {
            connections[i] = DriverManager.getConnection(CUSTOMER_SHARD_URLS[i], USERNAME, PASSWORD);
        }
        return connections;
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



// package com.example.util;

// import java.sql.Connection;
// import java.sql.DriverManager;
// import java.sql.SQLException;

// public class DatabaseConnection {
//     private static final String USER = "root";
//     private static final String PASSWORD = "mysql";

//     private static final String URL_SHARD1 = "jdbc:mysql://localhost:3306/usershard1";
//     private static final String URL_SHARD2 = "jdbc:mysql://localhost:3306/usershard2";
//     private static final String URL_SHARD3 = "jdbc:mysql://localhost:3306/usershard3";
    
//     private static final String URL = "jdbc:mysql://localhost:3306/";


//     static {
//         try {
//             Class.forName("com.mysql.cj.jdbc.Driver");
//         } catch (ClassNotFoundException e) {
//             throw new RuntimeException("Error loading MySQL Driver", e);
//         }
//     }

//     public static Connection getConnection(int userid) throws SQLException {
//         int shard = userid % 3;
//         switch (shard) {
//             case 0:
//                 return DriverManager.getConnection(URL_SHARD1, USER, PASSWORD);
//             case 1:
//                 return DriverManager.getConnection(URL_SHARD2, USER, PASSWORD);
//             case 2:
//                 return DriverManager.getConnection(URL_SHARD3, USER, PASSWORD);
//             default:
//                 throw new IllegalArgumentException("Invalid shard index");
//         }
//     }

//     public static Connection[] getConnection() {
//         try {
//             return new Connection[] {
//                 DriverManager.getConnection(URL_SHARD1, USER, PASSWORD),
//                 DriverManager.getConnection(URL_SHARD2, USER, PASSWORD),
//                 DriverManager.getConnection(URL_SHARD3, USER, PASSWORD)
//             };
//         } catch (SQLException e) {
//             throw new RuntimeException("Error connecting to user shards", e);
//         }
//     }

//     public static Connection[] getAllCabShardConnections() {
//         try {
//             return new Connection[] {
//                 DriverManager.getConnection("jdbc:mysql://localhost:3306/cabpositionshard1", USER, PASSWORD),
//                 DriverManager.getConnection("jdbc:mysql://localhost:3306/cabpositionshard2", USER, PASSWORD),
//                 DriverManager.getConnection("jdbc:mysql://localhost:3306/cabpositionshard3", USER, PASSWORD)
//             };
//         } catch (SQLException e) {
//             throw new RuntimeException("Failed to connect to cab shard", e);
//         }
//     }

//     public static Connection getCabShardConnection(int cabid) throws SQLException {
//         int shard = cabid % 3;
//         switch (shard) {
//             case 0: return DriverManager.getConnection("jdbc:mysql://localhost:3306/cabpositionshard1", USER, PASSWORD);
//             case 1: return DriverManager.getConnection("jdbc:mysql://localhost:3306/cabpositionshard2", USER, PASSWORD);
//             case 2: return DriverManager.getConnection("jdbc:mysql://localhost:3306/cabpositionshard3", USER, PASSWORD);
//             default: throw new IllegalArgumentException("Invalid shard for cabid: " + cabid);
//         }
//     }

//     public static Connection getConnection(String db) {
//         try {
//             return DriverManager.getConnection(URL+db, USER, PASSWORD);
//         } catch (SQLException e) {
//             e.printStackTrace();
//         }
//         return null;
//     }

//     // public static Connection getConnection() throws SQLException {
//     //     return DriverManager.getConnection(URL, USER, PASSWORD);
//     // }

// }










// package com.example.util;

// import java.sql.Connection;
// import java.sql.DriverManager;
// import java.sql.SQLException;

// public class DatabaseConnection {
//     private static final String URL = "jdbc:mysql://localhost:3306/zulacab";
//     private static final String USER = "root";
//     private static final String PASSWORD = "mysql";

//     //  Load MySQL JDBC Driver (optional if using JDBC 4.0+)
//     static {
//         try {
//             Class.forName("com.mysql.cj.jdbc.Driver");
//         } catch (ClassNotFoundException e) {
//             throw new RuntimeException("Error loading MySQL Driver", e);
//         }
//     }

//     // Method to get a new connection for each API call
//     public static Connection getConnection() throws SQLException {
//         return DriverManager.getConnection(URL, USER, PASSWORD);
//     }
// }
