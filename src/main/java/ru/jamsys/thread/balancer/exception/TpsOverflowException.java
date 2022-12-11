package ru.jamsys.thread.balancer.exception;

public class TpsOverflowException extends RuntimeException {

    public TpsOverflowException(String message) {
        super(message);
    }

}
