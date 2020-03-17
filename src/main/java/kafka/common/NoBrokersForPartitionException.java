package kafka.common;

public class NoBrokersForPartitionException extends RuntimeException{

    public NoBrokersForPartitionException() {
        super();
    }


    public NoBrokersForPartitionException(String message) {
        super(message);
    }
}
