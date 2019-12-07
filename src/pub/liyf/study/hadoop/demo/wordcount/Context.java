package pub.liyf.study.hadoop.demo.wordcount;

import java.util.HashMap;
import java.util.Map;

public class Context {
    private HashMap<Object, Object> contextMap = new HashMap<>();

    public void set(Object key, Object value){
        contextMap.put(key, value);
    }

    public Object get(Object key){
        return contextMap.get(key);
    }

    public Map<Object, Object> getContextMap(){
        return contextMap;
    }
}
