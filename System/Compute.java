package System;

import java.util.Map;
import java.util.Set;

public abstract class Compute<MessageDataType, OuterVariable> {
    public abstract MessageDataType process(Map<Integer, Set<Integer>> graph, Set<Integer> domain, OuterVariable var);
    //public abstract void process(Map<Integer, Set<Integer>> graph, Set<Integer> domain, Map<Integer, >)
}
