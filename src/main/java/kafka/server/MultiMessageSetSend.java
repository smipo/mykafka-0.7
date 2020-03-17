package kafka.server;

import kafka.network.ByteBufferSend;
import kafka.network.MultiSend;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

public class MultiMessageSetSend  extends MultiSend {

    List<MessageSetSend> sets;

    public ByteBuffer buffer ;
    public int allMessageSetSize ;
    public int expectedBytesToWrite;

    public MultiMessageSetSend(List<MessageSetSend> sets){
        super(MessageSetSend.messageSetSends());
        this.sets = sets;
        //todo
        for(MessageSetSend set:sets){
            this.allMessageSetSize += set.sendSize();
        }
        this.expectedBytesToWrite = 4 + 2 + allMessageSetSize;

        this.buffer = ((ByteBufferSend)sends().get(0)).buffer();
        this.buffer.putInt(2 + allMessageSetSize);
        this.buffer.putShort((short) 0);
        this.buffer.rewind();
    }

    public List<MessageSetSend> sets() {
        return sets;
    }
}
