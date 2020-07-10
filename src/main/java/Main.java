
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.*;

public class Main {

    private final static Logger LOGGER = LogManager.getLogger();

    public static void main(String[] args) {
        SparkSession sc = SparkSession.builder().master("local[*]").getOrCreate();

        LOGGER.info("**************************************** STARTING TRACE ***********************************");

        Dataset ds = sc.read().option("header", true).format("csv").load("src/main/resources/partitions.csv");

        LOGGER.error("Max reverse ordered partition: " + maxPartitionSpec(ds));

    }

    /**
     * So what this method does is collecting the list and
     * converting it to arraylist because what you need is something
     * comparable to be able to do a reverse order over the partitions
     * spec.
     * @param ds
     * @return all the partitions spec without order
     */
    private static ArrayList<TreeMap<String, String>> getPartitionSpecUnordered(Dataset ds) {

        return new ArrayList(ds.javaRDD().map(o ->
        {
            TreeMap map = new TreeMap<String, String>();
            // We then procceed to split every partition to add it to the list of partitions
            Arrays.stream(((Row) o).getString(0).split("/")).forEach(s -> {
                String[] split = s.split("=");
                // We save it as a tuple (which are comparable)
                map.put(split[0], split[1]);
            });
            // We return that list as o, then when we collect it,
            // it will be the list of lists of tuples.
            return map;
        }).collect());
    }

    /**
     * Function to order the list
     * @param ds
     * @return ordered partition spec list
     */
    private static ArrayList<TreeMap<String, String>> getPartitionSpecOrdered(Dataset ds) {

        ArrayList<TreeMap<String, String>> tuple = getPartitionSpecUnordered(ds);

        // USING ERROR SO WE CAN TRACE BETTER (IT HAS ANOTHER COLOR,
        // COULD BE DONE BY ENABLING TRACE AND CHANGING COLORS)

        // Printing the unordered list
        for (TreeMap<String, String> tuples : tuple) {
            LOGGER.error("This is the list: "+tuples.toString());
            tuples.keySet().forEach(z -> LOGGER.error("INITIAL ORDER APPLIED TO A " + z.toString() + "/"
                    + "INITIAL ORDER APPLIED TO B "+ tuples.get(z).toString()));
        }

        LOGGER.error("***********************BREAK*******************");

        // Reverse ordering the list, this is why it should be an ArrayList,
        // so it is comparable and not to have any cast problem
        Collections.reverse(tuple);

        // TODO: REVISE WHY THE REVERSE ORDER IS JUMPING WRONGLY ON
        // THE SECOND OPERATOR

        // Printing the ordered list
        for (TreeMap<String, String> tuples : tuple) {
            LOGGER.error("This is the list: "+tuples.toString());
            tuples.keySet().forEach(z -> LOGGER.error("FINAL ORDER APPLIED TO A " + z.toString() + "/"
                    + "FINAL ORDER APPLIED TO B "+ tuples.get(z).toString()));
        }

        return tuple;

    }

    /**
     * Method that returns the first reverse ordered partition
     * TODO: some tweaks might be needed to check the functionality,
     * it might not be fully working for multipartitions
     * @param ds
     * @return reverse ordered partition
     */
    private static String maxPartitionSpec(Dataset ds){
        ArrayList<TreeMap<String, String>> tuple = getPartitionSpecOrdered(ds);

        TreeMap<String, String> listToFilter= tuple.get(0);

        String ret="";

        // We form the most recent partition
        for (String keySet: listToFilter.keySet()) {
            ret+=keySet+"="+listToFilter.get(keySet)+",";
        }

        // We return it without the las comma
        return ret.substring(0, ret.length() - 1);
    }



}
