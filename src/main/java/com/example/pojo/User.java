package com.example.pojo;

import com.example.util.*;
import com.fasterxml.jackson.annotation.JsonProperty;

public class User {
    
    private int userid;
    private String name;
    private String username;

    @JsonProperty("password")
    private String encryptedpassword;
    private int age;
    private Gender gender;
    private Role role;

    public User() {
        
    }

    public User(int userid, String name, String username, String encryptedpassword, int age, Gender gender, Role role) {
        this.userid = userid;
        this.name = name;
        this.username = username;
        this.encryptedpassword = encryptedpassword;
        this.age = age;
        this.gender = gender;
        this.role = role;
    }



    public int getUserid() {
        return userid;
    }

    public void setUserid(int userid) {
        this.userid = userid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getEncryptedpassword() {
        return encryptedpassword;
    }

    public void setEncryptedpassword(String encryptedpassword) {
        this.encryptedpassword = encryptedpassword;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public Gender getGender() {
        return gender;
    }

    public void setGender(Gender gender) {
        this.gender = gender;
    }

    public Role getRole() {
        return role;
    }

    public void setRole(Role role) {
        this.role = role;
    }

    // @Override
    // public String toString() {
    //     StringBuilder sb = new StringBuilder();
    //     sb.append("User [");
    //     sb.append("userid=").append(userid).append(", ");
    //     sb.append("name=").append(name).append(", ");
    //     sb.append("encryptedpassword=").append(encryptedpassword).append(", ");
    //     sb.append("age=").append(age).append(", ");
    //     sb.append("gender=").append(gender).append(", ");
    //     sb.append("role=").append(role);
    //     sb.append("]");
    //     return sb.toString();
    // }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(userid).append(", ");
        sb.append(name).append(", ");
        sb.append(username).append(", ");
        sb.append(encryptedpassword).append(", ");
        sb.append(age).append(", ");
        sb.append(gender).append(", ");
        sb.append(role);
        return sb.toString();
    }


    
}



