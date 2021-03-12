package simpledb;

import java.util.Iterator;
import java.util.LinkedHashMap;

public class LRUCache<Key, Value> {

    private int capacity;
    private LinkedHashMap<Key, Value> map;

    public LRUCache(int capacity) {
        this.capacity = capacity;
        this.map = new LinkedHashMap<Key, Value>(16, 0.75f, true);
    }

    public Value get(Key key) {
        return this.map.get(key);
    }

    public boolean containsKey(Key key) {
        return this.map.containsKey(key);
    }

    public int size() {
        return this.map.size();
    }

    public void put(Key key, Value value) {
        if (!this.map.containsKey(key) && this.map.size() == this.capacity) {
            Iterator<Key> it = this.map.keySet().iterator();
            it.next();
            it.remove();
        }
        this.map.put(key, value);
    }
}