package kafka.network;

import kafka.server.MessageSetSend;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;
import java.util.Iterator;
import java.util.List;

public abstract class MultiSend<S extends Send> extends Send{

    private static Logger logger = Logger.getLogger(MultiSend.class);

    List<S> sends;

    int expectedBytesToWrite;
    private S current;
    private Iterator<S> curIte;
    int totalWritten = 0;

    public MultiSend(List<S> sends){
        this.sends = sends;
        this.curIte = sends.iterator();
        this.current = curIte.next();
    }

    public long writeTo(GatheringByteChannel channel) throws IOException {
        expectIncomplete();
        long written = current.writeTo(channel);
        totalWritten += written;
        if(current.complete())
            current = curIte.next();
        return written;
    }

    public boolean complete() {
        if (current == null) {
            if (totalWritten != expectedBytesToWrite)
                logger.error("mismatch in sending bytes over socket; expected: " + expectedBytesToWrite + " actual: " + totalWritten);
            return true;
        }
        else
            return false;
    }

    public List<S> sends() {
        return sends;
    }
}
