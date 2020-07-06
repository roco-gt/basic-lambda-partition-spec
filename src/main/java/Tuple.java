import java.io.Serializable;
import java.util.Comparator;

public class Tuple<A, B> implements Serializable, Comparable {

    public final Object a;
    public final Object b;

    public Tuple(A a, B b) {
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
    public int compareTo(Object o) {
        int res = this.a.toString().compareTo( ((Tuple<?,?>) o).a.toString());

        if(res==0){
            return this.b.toString().compareTo( ((Tuple<?,?>) o).b.toString());
        }

        return res;
    }
}