package System;

import scala.Serializable;

import java.util.Vector;

public class Message<dataType> implements Serializable {
    public int msg_var = -1;
    public dataType data;
    //public abstract void process();
    public dataType getData(){ return data;}
}
