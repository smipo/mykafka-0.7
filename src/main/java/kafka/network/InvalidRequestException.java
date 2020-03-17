package kafka.network;

public class InvalidRequestException extends RuntimeException {

    public InvalidRequestException() {
        super();
    }


    public InvalidRequestException(String message) {
        super(message);
    }
}
