package com.example.util;

import com.example.pojo.User;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;

public class AuthUtil {

    // Validates session and Role then returns the logged-in user
    public static User validateSession(HttpServletRequest request, Role requiredRole) {
        HttpSession session = request.getSession(false);

        if (session == null || session.getAttribute("user") == null) {
            throw new SecurityException("Login required");
        }

        User user = (User) session.getAttribute("user");

        if (user.getRole() == null || user.getRole() != requiredRole) {
            throw new SecurityException("Access denied: " + requiredRole + " role required");
        }

        return user;
    }

}
