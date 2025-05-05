package com.example.websocket;

import java.util.*;
import java.util.concurrent.*;

import jakarta.websocket.OnOpen;
import jakarta.websocket.OnClose;
import jakarta.websocket.OnMessage;
import jakarta.websocket.Session;
import jakarta.websocket.server.PathParam;
import jakarta.websocket.server.ServerEndpoint;

@ServerEndpoint("/chat/{roomId}/{role}/{userid}")
public class ChatEndpoint {

    // Map<roomId, List<Session>>
    private static final Map<String, Set<Session>> chatRooms = new ConcurrentHashMap<>();


    @OnOpen
    public void onOpen(Session session, @PathParam("roomId") String roomId, @PathParam ("role") String role, @PathParam("userid") String userid) {
     
        chatRooms.computeIfAbsent(roomId, k -> ConcurrentHashMap.newKeySet()).add(session);
        // session.getUserProperties().put("roomid", roomId);
        // session.getUserProperties().put("role", role);
        // session.getUserProperties().put("userid", userid);
        System.out.println(session);

    }

    @OnMessage
    public void onMessage(String message, Session sender, @PathParam("roomId") String roomId) {

        // Broadcast to all users in the same room
        for (Session session : chatRooms.getOrDefault(roomId, Set.of())) {
            if (session.isOpen()) {
                session.getAsyncRemote().sendText(message);
            }
        }
    }

    @OnClose
    public void onClose(Session session, @PathParam("roomId") String roomId, @PathParam ("role") String role, @PathParam("userid") String userid) {
        // if(Role.valueOf((String) session.getUserProperties().get("role")) == Role.CAB){
        //     DriverSocket.sendCloseRequest((String) session.getUserProperties().get("userid"));
        // }
        Set<Session> roomSessions = chatRooms.get(roomId);
        if (roomSessions != null) {
            roomSessions.remove(session);
            if (roomSessions.isEmpty()) {
                chatRooms.remove(roomId);
            }
        }
        System.out.println("CLOSE CONNECTION "+ session);
        System.out.println(roomSessions.isEmpty());
    }
}
