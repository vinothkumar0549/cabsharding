package com.example.controller;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
// import jakarta.servlet.http.Cookie;
import org.json.*;
import com.example.database.DatabaseStorage;
import com.example.database.Storage;
import com.example.pojo.CabPositions;
import com.example.pojo.CustomerAck;
import com.example.pojo.Penalty;
import com.example.pojo.Ride;
import com.example.pojo.TotalSummary;
import com.example.pojo.User;
import com.example.service.CabService;
import com.example.util.AuthUtil;
import com.example.util.Gender;
import com.example.util.Role;
import com.example.util.RoomIdGenerator;
import com.example.websocket.DriverSocket;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

@Path("/cab")
public class ZulacabController {

    Storage storage = new DatabaseStorage();

    CabService cabservice = new CabService(storage);

    @GET
    @Path("/hello")
    @Produces(MediaType.APPLICATION_JSON)
    public Response hello() {
        return Response.status(Response.Status.OK).entity("Hello World").build();
    }

    @POST
    @Path("/register")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response register(String RequestBody, @Context HttpServletRequest request) {

        try {
            // Parse the incoming JSON request
            JSONObject json = new JSONObject(RequestBody);

            // Extract user-related data
            JSONObject userJson = json.getJSONObject("user");

            String name = userJson.getString("name");
            String username = userJson.getString("username");
            String password = userJson.getString("password");

            int age = userJson.getInt("age");
            Gender gender = Gender.valueOf(userJson.getString("gender"));
            Role role = Role.valueOf(userJson.getString("role"));
            // String adminusername = null;
            // String adminpassword = null;
            String cablocation = null;
            String cabtype = null;
            if( role == Role.CAB) {
                // Extract userid and password
            // adminusername = json.getString("adminusername");
            // adminpassword = json.getString("adminpassword");
            AuthUtil.validateSession(request, Role.ADMIN);
            cablocation = json.getString("cablocation");
            cabtype = json.getString("cabtype");
            //adminpassword = cabservice.encrypt(adminpassword, 1);
            }

            // You can now create a User object using the extracted data
            User user = new User();
            user.setUsername(username);
            user.setEncryptedpassword(cabservice.encrypt(password, 1));
            user.setName(name);
            user.setAge(age);
            user.setGender(gender);
            user.setRole(role);
            
            // Call the service to register the user
            int id = cabservice.register(user, cablocation, cabtype);

            return Response.status(Response.Status.OK).entity("{\"userid\": \"" + id + "\"}").build();

        } catch (BadRequestException e) {
            return Response.status(Response.Status.BAD_REQUEST).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (SecurityException e) {
            return Response.status(Response.Status.UNAUTHORIZED).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        }
        
    }

    @POST
    @Path("/login")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response login(String RequestBody, @Context HttpServletRequest request) {

        JSONObject json = new JSONObject(RequestBody);
        String username = json.getString("username");
        String password = json.getString("password");

        if(username == null || password == null){
            return Response.status(Response.Status.BAD_REQUEST).entity("{\"error\": \"Invalid Arguments\"}").build();
        }

        try {

            User user = cabservice.login(username, cabservice.encrypt(password, 1));

            HttpSession session = request.getSession(true); // create session if not exists
            session.setAttribute("user", user); // store user object (or userId)
            session.setAttribute("role", user.getRole());

            return Response.status(Response.Status.OK).entity(user).build();
        } catch (BadRequestException e) {
            return Response.status(Response.Status.BAD_REQUEST).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (SecurityException e) {
            return Response.status(Response.Status.UNAUTHORIZED).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        }
        
    }

    @POST
    @Path("/addlocation")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response addlocation(String RequestBody, @Context HttpServletRequest request) {

        JSONObject json = new JSONObject(RequestBody);

        // String adminusername = json.getString("adminusername");
        // String adminpassword = json.getString("adminpassword");
        String locationname = json.getString("locationname");
        int distance = json.getInt("distance");

        if(locationname == null || distance < 0){
            throw new IllegalArgumentException("Invalid input");
        }
        
        try {
            
            AuthUtil.validateSession(request, Role.ADMIN);

            int locationid = cabservice.addlocation( locationname, distance);
            return Response.status(Response.Status.OK).entity("{\"locationid\": \"" + locationid + "\"}").build();

        } catch (BadRequestException e) {
            return Response.status(Response.Status.BAD_REQUEST).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (SecurityException e) {
            return Response.status(Response.Status.UNAUTHORIZED).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        }

    }

    @POST
    @Path("/removelocation")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response removelocation(String RequestBody, @Context HttpServletRequest request) {

        JSONObject json = new JSONObject(RequestBody);

        // String adminusername = json.getString("adminusername");
        // String adminpassword = json.getString("adminpassword");
        String locationname = json.getString("locationname");
        int distance = json.getInt("distance");

        if(locationname == null || distance < 0){
            throw new IllegalArgumentException("Invalid input");
        }
        
        try {

            AuthUtil.validateSession(request, Role.ADMIN);

            String message = cabservice.removelocation(locationname, distance);
            return Response.status(Response.Status.OK).entity("{\"message\": \"" + message + "\"}").build();

        } catch (BadRequestException e) {
            return Response.status(Response.Status.BAD_REQUEST).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (SecurityException e) {
            return Response.status(Response.Status.UNAUTHORIZED).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        }

    }

    @POST
    @Path("/checkavailablecab")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response checkavialablecab(String RequestBody, @Context HttpServletRequest request) {
        // JSONObject json = new JSONObject(RequestBody);
        // String customerusername = json.getString("customerusername");
        // String customerpassword = json.getString("customerpassword");

        try {
            AuthUtil.validateSession(request, Role.CUSTOMER);
            List<CabPositions> availablecabs = cabservice.checkavailablecab();
            return Response.status(Response.Status.OK).entity(Map.of("availablecabs", availablecabs)).build();

        } catch (BadRequestException e) {
            return Response.status(Response.Status.BAD_REQUEST).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (SecurityException e) {
            return Response.status(Response.Status.UNAUTHORIZED).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (IllegalArgumentException e) {
            return Response.status(Response.Status.BAD_REQUEST).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (IllegalStateException e) {
            return Response.status(Response.Status.NOT_FOUND).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        }
    }

    @POST
    @Path("/bookcab")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response bookcab(String RequestBody, @Context HttpServletRequest request) {
        JSONObject json = new JSONObject(RequestBody);
        // String customerusername = json.getString("customerusername");
        // String customerpassword = json.getString("customerpassword");
        String source = json.getString("source");
        String destination = json.getString("destination");
        String cabtype = json.getString("cabtype");
        
        // Extract datetime strings from JSON
        String departureTimeStr = json.getString("departuretime");
        String arrivalTimeStr = json.getString("arrivaltime");

        // Convert string to LocalDateTime
        LocalDateTime departuretime = LocalDateTime.parse(departureTimeStr);
        LocalDateTime arrivaltime = LocalDateTime.parse(arrivalTimeStr);

        

        try {
            if (departuretime.isBefore(LocalDateTime.now())) {
                throw new BadRequestException("Departure Date and Time not Valid");
            }
            if(arrivaltime.isBefore(departuretime)) {
                throw new BadRequestException("Arrival Date and Time not Valid");
            }
            User customer = AuthUtil.validateSession(request, Role.CUSTOMER);
            CustomerAck customerAck = cabservice.bookcab(customer, source, destination, cabtype, departuretime, arrivaltime);
            String roomid = RoomIdGenerator.generateRoomId();
            customerAck.setRoomid(roomid);
            DriverSocket.sendRideAssignment(String.valueOf(customerAck.getCabid()), customerAck.getRoomid(), source, destination); 
            return Response.status(Response.Status.OK).entity(customerAck).build();

        } catch (BadRequestException e) {
            return Response.status(Response.Status.BAD_REQUEST).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (SecurityException e) {
            return Response.status(Response.Status.UNAUTHORIZED).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (IllegalArgumentException e) {
            return Response.status(Response.Status.BAD_REQUEST).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (IllegalStateException e) {
            return Response.status(Response.Status.NOT_FOUND).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        }
    }

    @POST
    @Path("/rideconfirmation")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response rideconfirmation(String RequestBody, @Context HttpServletRequest request) {
        JSONObject json = new JSONObject(RequestBody);
        // String customerusername = json.getString("customerusername");
        // String customerpassword = json.getString("customerpassword");
        int cabid = json.getInt("cabid");
        int distance = json.getInt("distance");
        boolean confirm = json.getBoolean("confirm");
        String source = json.getString("source");
        String destination = json.getString("destination");

        // Extract datetime strings from JSON
        String departureTimeStr = json.getString("departuretime");
        String arrivalTimeStr = json.getString("arrivaltime");  
        // Convert string to LocalDateTime
        LocalDateTime departuretime = LocalDateTime.parse(departureTimeStr);
        LocalDateTime arrivaltime = LocalDateTime.parse(arrivalTimeStr);

        try {
            if (departuretime.isBefore(LocalDateTime.now())) {
                throw new BadRequestException("Departure Date and Time not Valid");
            }
            if(arrivaltime.isBefore(departuretime)) {
                throw new BadRequestException("Arrival Date and Time not Valid");
            }
            User customer = AuthUtil.validateSession(request, Role.CUSTOMER);
            if(confirm){
                int id = cabservice.confirmride(customer, cabid, distance, source, destination, departuretime, arrivaltime);
                return Response.status(Response.Status.OK).entity("{\"cabid\": \"" + id + "\"}").build();
            }
            throw new IllegalArgumentException("Ride Cancelled");
        } catch (BadRequestException e) {
            return Response.status(Response.Status.BAD_REQUEST).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (SecurityException e) {
            return Response.status(Response.Status.UNAUTHORIZED).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (IllegalArgumentException e) {
            return Response.status(Response.Status.BAD_REQUEST).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (IllegalStateException e) {
            return Response.status(Response.Status.NOT_FOUND).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        }
    }

    @POST
    @Path("/cancelride")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response cancelride(String RequestBody, @Context HttpServletRequest request) {
        JSONObject json = new JSONObject(RequestBody);
        // String customerusername = json.getString("customerusername");
        // String customerpassword = json.getString("customerpassword");
        int cabid = json.getInt("cabid");
        int customerid = json.getInt("customerid");
        // int distance = json.getInt("distance");
        // boolean confirm = json.getBoolean("confirm");
        // String source = json.getString("source");
        // String destination = json.getString("destination");

        try {

            AuthUtil.validateSession(request, Role.CUSTOMER);
            cabservice.cancelride(cabid, customerid);
            return Response.status(Response.Status.OK).entity("{\"cancel\": \"" + cabid + "\"}").build();
            
        } catch (BadRequestException e) {
            return Response.status(Response.Status.BAD_REQUEST).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (SecurityException e) {
            return Response.status(Response.Status.UNAUTHORIZED).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (IllegalArgumentException e) {
            return Response.status(Response.Status.BAD_REQUEST).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (IllegalStateException e) {
            return Response.status(Response.Status.NOT_FOUND).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        }
    }


    @POST
    @Path("/customersummary")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response customersummary(String RequestBody, @Context HttpServletRequest request) {
        // JSONObject json = new JSONObject(RequestBody);
        // String customerusername = json.getString("customerusername");
        // String customerpassword = json.getString("customerpassword");

        try {
            User customer = AuthUtil.validateSession(request, Role.CUSTOMER);
            List<Ride> customerrides = cabservice.customerSummary(customer);
            return Response.status(Response.Status.OK).entity(Map.of("customersummary", customerrides)).build();

        } catch (BadRequestException e) {
            return Response.status(Response.Status.BAD_REQUEST).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (SecurityException e) {
            return Response.status(Response.Status.UNAUTHORIZED).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (IllegalArgumentException e) {
            return Response.status(Response.Status.BAD_REQUEST).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (IllegalStateException e) {
            return Response.status(Response.Status.NOT_FOUND).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        }
    }

    @POST
    @Path("/penalty")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response penalty(String RequestBody, @Context HttpServletRequest request) {
        // JSONObject json = new JSONObject(RequestBody);
        // String customerusername = json.getString("customerusername");
        // String customerpassword = json.getString("customerpassword");

        try {
            User customer = AuthUtil.validateSession(request, Role.CUSTOMER);
            List<Penalty> penalties = cabservice.getpenalty(customer);
            return Response.status(Response.Status.OK).entity(Map.of("penalty", penalties)).build();

        } catch (BadRequestException e) {
            return Response.status(Response.Status.BAD_REQUEST).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (SecurityException e) {
            return Response.status(Response.Status.UNAUTHORIZED).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (IllegalArgumentException e) {
            return Response.status(Response.Status.BAD_REQUEST).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (IllegalStateException e) {
            return Response.status(Response.Status.NOT_FOUND).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        }
    }

    @POST
    @Path("/cabsummary")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response cabsummary(String RequestBody, @Context HttpServletRequest request) {
        // JSONObject json = new JSONObject(RequestBody);
        // String cabusername = json.getString("cabusername");
        // String cabpassword = json.getString("cabpassword");

        try {
            User cab = AuthUtil.validateSession(request, Role.CAB);
            List<Ride> cabrides = cabservice.cabSummary(cab);
            return Response.status(Response.Status.OK).entity(Map.of("cabsummary", cabrides)).build();

        } catch (BadRequestException e) {
            return Response.status(Response.Status.BAD_REQUEST).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (SecurityException e) {
            return Response.status(Response.Status.UNAUTHORIZED).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (IllegalArgumentException e) {
            return Response.status(Response.Status.BAD_REQUEST).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (IllegalStateException e) {
            return Response.status(Response.Status.NOT_FOUND).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        }
    }

    @POST
    @Path("/getallcabsummary")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getallcabsummary(String RequestBody, @Context HttpServletRequest request) {
        //JSONObject json = new JSONObject(RequestBody);
        // String adminusername = json.getString("adminusername");
        // String adminpassword = json.getString("adminpassword");

        try {
            AuthUtil.validateSession(request, Role.ADMIN);
            List<List<Ride>> allcabridesummary = cabservice.getallcabsummary();
            List<TotalSummary> totalcabsummary = cabservice.gettotalcabsummary();
            Map<String, Object> responseMap = new HashMap<>();
            responseMap.put("cabsummary", allcabridesummary);
            responseMap.put("totalcabsummary", totalcabsummary);
            return Response.status(Response.Status.OK).entity(responseMap).build();

        } catch (BadRequestException e) {
            return Response.status(Response.Status.BAD_REQUEST).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (SecurityException e) {
            return Response.status(Response.Status.UNAUTHORIZED).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (IllegalArgumentException e) {
            return Response.status(Response.Status.BAD_REQUEST).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (IllegalStateException e) {
            return Response.status(Response.Status.NOT_FOUND).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        }
    }

    @POST
    @Path("/getallcustomersummary")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getallcustomersummary(String RequestBody, @Context HttpServletRequest request) {
        // JSONObject json = new JSONObject(RequestBody);
        // String adminusername = json.getString("adminusername");
        // String adminpassword = json.getString("adminpassword");

        try {
            AuthUtil.validateSession(request, Role.ADMIN);
            List<List<Ride>> allcustomerridesummary = cabservice.getallcustomersummary();
            List<TotalSummary> totalcustomersummary = cabservice.gettotalcustomersummary();
            Map<String, Object> responseMap = new HashMap<>();
            responseMap.put("customersummary", allcustomerridesummary);
            responseMap.put("totalcustomersummary", totalcustomersummary);
            return Response.status(Response.Status.OK).entity(responseMap).build();

        } catch (BadRequestException e) {
            return Response.status(Response.Status.BAD_REQUEST).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (SecurityException e) {
            return Response.status(Response.Status.UNAUTHORIZED).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (IllegalArgumentException e) {
            return Response.status(Response.Status.BAD_REQUEST).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (IllegalStateException e) {
            return Response.status(Response.Status.NOT_FOUND).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("{\"error\": \"" + e.getMessage() + "\"}").build();
        }
    }

    // @POST
    // @Path("/logout")
    // public Response logout(@Context HttpServletRequest request) {
    //     HttpSession session = request.getSession(false);
    //     if (session != null) {
    //         session.invalidate();
    //     }
    //     return Response.ok("Logged out successfully").build();
    // }

    @POST
    @Path("/logout")
    public Response logout(String RequestBody, @Context HttpServletRequest request, @Context HttpServletResponse response) {
        JSONObject json = new JSONObject(RequestBody);
        int userid = json.getInt("userid");
        HttpSession session = request.getSession(false);
        if (session != null) {
            session.invalidate();
        }
        response.addCookie(cabservice.logout(userid));
    
        // // Instruct browser to delete the JSESSIONID cookie
        // Cookie cookie = new Cookie("JSESSIONID", null);
        // cookie.setMaxAge(0);         // Expire the cookie
        // cookie.setPath("/cab");         // IMPORTANT: must match original path
        // cookie.setHttpOnly(true);    // Optional but good practice
        // response.addCookie(cookie);  // Send the deletion command

        return Response.ok("{\"message\": \"Logout Successfully\"}").build();
    }

}


// String query = "SELECT cp.cabid, ABS(src.distance - dest.distance) AS total_distance FROM cabpositions cp\r\n" + //
//                         "JOIN locations cl ON cp.locationid = cl.locationid\r\n" + //
//                         "JOIN locations src ON src.locationname = ? \r\n" + //
//                         "JOIN locations dest ON dest.locationname = ?\r\n" + //
//                         "ORDER BY ABS(cl.distance - src.distance)\r\n" + //
//                         "LIMIT 1;\r\n";


// String query = "SELECT cp.cabid, \r\n" +
//                         "ABS(src.distance - dest.distance) AS total_distance,\r\n" +
//                         "COUNT(rd.rideid) AS trip_count\r\n" + 
//                         "FROM cabpositions cp\r\n" + 
//                         "JOIN locations cl ON cp.locationid = cl.locationid\r\n" + 
//                         "JOIN locations src ON src.locationname = ?\r\n" + 
//                         "JOIN locations dest ON dest.locationname = ?\r\n" + 
//                         "LEFT JOIN ridedetails rd ON cp.cabid = rd.cabid\r\n" + 
//                         "GROUP BY cp.cabid, cl.distance, src.distance, dest.distance\r\n" + 
//                         "ORDER BY ABS(cl.distance - src.distance) ASC, trip_count ASC\r\n" + 
//                         "LIMIT 1;";
