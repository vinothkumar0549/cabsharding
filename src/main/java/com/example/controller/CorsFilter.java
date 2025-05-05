package com.example.controller;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.ext.Provider;
import java.io.IOException;

@Provider
public class CorsFilter implements ContainerResponseFilter {

    @Override
        public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext) throws IOException {
                String origin = requestContext.getHeaderString("Origin");
                if (origin != null && origin.equals("http://localhost:3000")) {
                        responseContext.getHeaders().add("Access-Control-Allow-Origin", origin); // Not "*"
                        responseContext.getHeaders().add("Access-Control-Allow-Credentials", "true"); // Allow cookies
                        responseContext.getHeaders().add("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
                        responseContext.getHeaders().add("Access-Control-Allow-Headers", "Content-Type, Accept, Authorization");
                }
        }
                
}