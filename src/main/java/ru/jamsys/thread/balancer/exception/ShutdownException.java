package ru.jamsys.thread.balancer.exception;

public class ShutdownException extends RuntimeException{

    public ShutdownException(String message) {
        super(message);
    }

}
