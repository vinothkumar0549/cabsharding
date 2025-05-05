package com.example.websocket;

import jakarta.websocket.*;
import jakarta.websocket.server.PathParam;
import jakarta.websocket.server.ServerEndpoint;
import java.util.concurrent.ConcurrentHashMap;

@ServerEndpoint("/cab/{cabid}")
public class DriverSocket {

    // Map to store connected drivers (driverId -> session)
    private static ConcurrentHashMap<String, Session> driverSessions = new ConcurrentHashMap<>();

    @OnOpen
    public void onOpen(Session session, @PathParam("cabid") String cabid) {
        driverSessions.put(cabid, session);
    }

    @OnMessage
    public void onMessage(String message, @PathParam("cabid") String cabid) {
        // Usually drivers don’t send messages here unless responding.
    }

    @OnClose
    public void onClose(Session session, @PathParam("cabid") String cabid) {
        driverSessions.remove(cabid);
    }

    @OnError
    public void onError(Session session, Throwable throwable, @PathParam("cabid") String cabid) {
        System.out.println("Error with Cab " + cabid + ": " + throwable.getMessage());
    }

    // Static method to send a message to a driver
    public static void sendRideAssignment(String cabid, String roomId, String source, String destination) {
        Session session = driverSessions.get(cabid);
        if (session != null && session.isOpen()) {
            String jsonMessage = String.format(
                "{\"type\":\"ASSIGNED\", \"roomId\":\"%s\", \"source\":\"%s\", \"destination\":\"%s\" }",
                roomId, source, destination
            );
            try {
                session.getBasicRemote().sendText(jsonMessage);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("Cab " + cabid + " not connected.");
        }
    }

    public static void sendCloseRequest(String cabid) {
        Session session = driverSessions.get(cabid);
        if (session != null && session.isOpen()) {
            String jsonMessage = String.format(
                "{\"type\":\"CONNECTIONCLOSE\"}"
            );
            try {
                session.getBasicRemote().sendText(jsonMessage);
                System.out.println("CAB CLOSED " + cabid);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            System.out.println(cabid +" not open connection");
        }
    }
}

