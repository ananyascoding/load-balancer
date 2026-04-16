package com.lbsim;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class LoadBalancerApp {
    public static void main(String[] args) {
        SpringApplication.run(LoadBalancerApp.class, args);
    }
}
