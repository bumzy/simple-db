package simpledb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

public class LRUCacheTest {

    /**
     * Unit test for LRUCache empty
     */
    @Test public void testEmpty() {
        LRUCache<Integer, Integer> c = new LRUCache<Integer, Integer>(2);
        assertEquals(c.size(), 0);
        assertEquals(c.containsKey(1), false);
        assertEquals(c.get(1), null);
    }

    /**
     * Unit test for LRUCache below capacity
     */
    @Test public void testBelowCapacity() {
        LRUCache<Integer, Integer> c = new LRUCache<Integer, Integer>(2);
        c.put(1, 1);
        assertTrue(c.get(1) == 1);
        assertTrue(c.get(2) == null);
        c.put(2, 4);
        assertTrue(c.get(1) == 1);
        assertTrue(c.get(2) == 4);
    }

    /**
     * Unit test for LRUCache above capacity, oldest is removed
     */
    @Test public void testAboveCapacity() {
        LRUCache<Integer, Integer> c = new LRUCache<Integer, Integer>(2);
        c.put(1, 1);
        c.put(2, 4);
        c.put(3, 9);
        assertTrue(c.get(1) == null);
        assertTrue(c.get(2) == 4);
        assertTrue(c.get(3) == 9);
    }

    /**
     * Unit test for LRUCache get renews entry
     */
    @Test public void testGetRenewsEntry() {
        LRUCache<Integer, Integer> c = new LRUCache<Integer, Integer>(2);
        c.put(1, 1);
        c.put(2, 4);
        assertTrue(c.get(1) == 1);
        c.put(3, 9);
        assertTrue(c.get(1) == 1);
        assertTrue(c.get(2) == null);
        assertTrue(c.get(3) == 9);
    }

    /**
     * Unit test for LRUCache Double put does not remove due to capacity
     */
    @Test public void testDoublePut() {
        LRUCache<Integer, Integer> c = new LRUCache<Integer, Integer>(2);
        assertTrue(c.get(2) == null);
        c.put(2, 6);
        assertTrue(c.get(1) == null);
        c.put(1, 5);
        c.put(1, 2);
        assertTrue(c.get(1) == 2);
        assertTrue(c.get(2) == 6);
    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
      return new JUnit4TestAdapter(LRUCacheTest.class);
    }
}