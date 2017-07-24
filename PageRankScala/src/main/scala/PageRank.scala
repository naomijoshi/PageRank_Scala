import com.pagerank.Bz2WikiParser
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Naomi on 10/30/16.
  */
object PageRank {
  def main(args: Array[String]) = {
    try {
      if (args.length < (2)) {
        System.err.println("Usage: SparkPageRank <inputpath> <outputpath>")
        System.exit(1)
      }
      val conf = new SparkConf().setMaster("local").setAppName ("PageRank").set("spark.default.parallelism","30")
      val sc = new SparkContext(conf)
      val parser: Bz2WikiParser = new Bz2WikiParser
      //Takes the input parser and applies java parser function to each line of it. Also filters null values
      val input = sc.textFile(args(0),20).map(l => parser.parse(l)).filter(line => line != null).persist()
      //Calculate initial page rank
      val initalRank = 1.toDouble./(input.count().toDouble)
      //Change the parsed input format by splitting it by ":" and also assigning the initial page rank to RDD
      var value = input.map(line => line.split(":")).map(fields => (fields.apply(0), (fields.apply(1), initalRank)))

      //10 Iterations
      for (i <- 1 to 10) {
        //Calculate node everytime as some dangling nodes are removed in each iteration
        val nodes = value.count().toDouble
        //Filter nodes with blank adjacency list and calculate tha dangling mass using map and add all of it using reduce
        val dangling = value.filter(f => "[]".equals(f._2._1))
          .map(fields => fields._2)
          .flatMap(t => t._1.substring(1, t._1.size - 1).split(",")
            .map(l => (l.trim, t._2 / nodes)))
          .map(l => l._2).reduce((pr, sum) => pr + sum);

        //Filter nodes which have outlinks, calculate the pagerank contribution for each outlink using map,
        //add all the contributions for a particular node using reduceByKey, apply Pagerank formula to the values using
        //mapValues and also add the calculated dangling mass
        val rank = value.filter(f => !"[]".equals(f._2._1))
          .map(fields => fields._2)
          .flatMap(t => t._1.substring(1, t._1.size.-(1)).split(",")
            .map(l => (l.trim, t._2./(t._1.substring(1, t._1.size - 1).split(",").size))))
          .reduceByKey((pr, sum) => pr + sum)
          .mapValues(sum => 0.15 / nodes + 0.85 * (sum + dangling))
          .map(fields => (fields._1.trim, fields._2))

        //replace the existing input RDD with new Pageranks and run next iteration
        value = value.join(rank).map(l => (l._1, (l._2._1._1, l._2._2)))
      }
      //Changed the RDD structure to make pagerank as Key, Picked top 100 values based on key PR locally,
      //repartitioned those values to 1 partition and then sort it globally before saving to file
      sc.parallelize(value.map(line => (line._2._2, line._1))
        .top(100)).repartition(1).sortByKey(false).map(l => l.swap)saveAsTextFile (args(1))
      sc.stop()
    }
    catch { case _: Throwable =>}
  }
}
