import org.apache.spark.sql.SparkSession
import java.io._

/*def isCardSearch(str: String) : Boolean = {

}*/

object TestApp extends App {
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Sample App")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  val path = "DataSets\\ConsSession\\"
  val files = new File(path).listFiles.map(_.getName).toList
  var count : Int = 0

  files.foreach(file=>{
    val data = spark.sparkContext.textFile(path + file)
    var cs_status : Boolean = false
    data.collect.foreach(line => {
      if(cs_status){
        if(line.indexOf("ACC_45616") != -1){
          count+=1
        }
        cs_status = false
      }
      if(line.indexOf("CARD_SEARCH_END") != -1){
        cs_status = true
      }
    })
  })

  println(count)

  spark.stop()
}