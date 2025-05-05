package com.example.util;

import java.util.concurrent.atomic.AtomicInteger;

public class RoomIdGenerator {
    // Starting from 1000 (or any other number)
    private static final AtomicInteger counter = new AtomicInteger(1000); 

    public static String generateRoomId() {
        // Increment the counter and return the roomId
        return "room_" + counter.getAndIncrement();
    }
}

