package org.apdplat.counter.valve;

/**
 * Created by ysc on 1/9/2017.
 */
public class Counter {
    private String path;
    private long delta;

    public Counter(String path, long delta) {
        this.path = path;
        this.delta = delta;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public long getDelta() {
        return delta;
    }

    public void setDelta(long delta) {
        this.delta = delta;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Counter counter = (Counter) o;

        if (delta != counter.delta) return false;
        return path != null ? path.equals(counter.path) : counter.path == null;

    }

    @Override
    public int hashCode() {
        int result = path != null ? path.hashCode() : 0;
        result = 31 * result + (int) (delta ^ (delta >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "Counter{" +
                "path='" + path + '\'' +
                ", delta=" + delta +
                '}';
    }
}
