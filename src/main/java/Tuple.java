import java.io.Serializable;
import java.util.Comparator;

public class Tuple<A, B> implements Serializable, Comparable {

    public final Comparable a;
    public final Comparable b;

    public Tuple(Comparable a, Comparable b) {
        this.a = a;
        this.b = b;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Tuple<?, ?> tuple = (Tuple<?, ?>) o;
        if (!a.equals(tuple.a)) return false;
        return b.equals(tuple.b);
    }

    @Override
    public int hashCode() {
        int result = a.hashCode();
        result = 31 * result + b.hashCode();
        return result;
    }

    @Override
    /**
     * Made for string comparison, focused on
     */
    public int compareTo(Object o) {
        Comparable comparable = (Comparable) o;
        int res = this.a.compareTo( ((Tuple<?,?>) o).a);

        if(res==0){
            return this.b.compareTo( ((Tuple<?,?>) o).b);
        }

        return res;
    }
}