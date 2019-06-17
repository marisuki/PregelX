package System;


import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.PriorityBlockingQueue;

public abstract class Combiner<MessageDataType, AnsDataType> {
    public LinkedBlockingDeque<Message<MessageDataType>> messageQueue = new LinkedBlockingDeque<>();
    public abstract AnsDataType combine();
}
