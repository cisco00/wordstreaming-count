
import org.apache.spark._
import org.apache.spark.streaming._

val ssc = new StreamingContext(sc, Seconds(10))

val lines = ssc.socketTextStream("localHost", 9999)

val words = lines.flatMap(_.split(" "))

val pairs = words.map(word => (word,1))
val wordcount = pairs.reduceByKey(_*_)

wordcount.print()

ssc.start()


