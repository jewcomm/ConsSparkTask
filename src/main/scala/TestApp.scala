import org.apache.spark.sql.SparkSession
import java.io._

object TestApp extends App {
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Sample App")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  val path = "DataSets\\ConsSession\\"
  val files = new File(path).listFiles.map(_.getName).toList
  var count : Int = 0 // счетчик

  files.foreach(file=>{
    val data = spark.sparkContext.textFile(path + file)
    var cs_status : Boolean = false // статус найдена ли нужная строка
    /*
    * так как результаты поиска выводятся на следующей строке после "CARD_SEARCH_END"
    * то устанавливаем в true статус
    * в следующей итерации ищем есть ли в строке нужный идентификатор
    * статус устанавливаем обратно в false в любом случае
    * */
    data.collect.foreach(line => {
      if(cs_status){ // если нашли
        if(line.indexOf("ACC_45616") != -1){ // если нашли нужный документ, увличиваем счетчик
          count+=1
        }
        cs_status = false
      }
      if(line.indexOf("CARD_SEARCH_END") != -1){ // ищем нужное выражение
        cs_status = true // статус переводим в true
      }
    })
  })

  println("in the search card, the document ACC_45616 was searched for: " + count)

  spark.stop()
}