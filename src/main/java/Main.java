
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Main {

    private final static Logger LOGGER = LogManager.getLogger();

    public static void main(String[] args) {
        SparkSession sc = SparkSession.builder().master("local[*]").getOrCreate();

        LOGGER.info("**************************************** STARTING TRACE ***********************************");

        List<Tuple<String,String>> tuple =sc.read().option("header",true).format("csv").load("src/main/resources/partitions.csv")
                .javaRDD().map(s-> new Tuple<String, String>(s.getString(0).split("/")[0].split("=")[1],
                s.getString(0).split("/")[1].split("=")[1])).collect();

        ArrayList<Tuple<String,String>> tuple2 = new ArrayList<>(tuple);

        // USING ERROR SO WE CAN TRACE BETTER

        tuple2.stream().map(s-> "INITIAL ORDER"+s.a.toString()+"/"+s.b.toString()).forEach(LOGGER::error);

        Collections.sort(tuple2, Collections.reverseOrder());

        tuple2.stream().map(s-> "REVERSE ORDER APPLIED"+s.a.toString()+"/"+s.b.toString()).forEach(LOGGER::error);


    }

}
