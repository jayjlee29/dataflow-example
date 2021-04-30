package co.vible.dataflow.models;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class CouchbaseModel implements Serializable {

    Map<String, Object> map = new HashMap();

    public Map<String, Object> getMap(){
        return map;
    }

    public void setMap(Map m){
        map = m;
    }
    
}
