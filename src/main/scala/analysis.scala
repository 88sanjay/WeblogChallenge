import org.apache.spark.{SparkContext,SparkConf,SQLContext}
import org.apache.spark.sql.SQLContext.implicits._
import scala.collection.mutable.{Map => MutableMap}

object Spark {
  val sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local[*]"))
}

object LogParser extends java.io.Serializable {
    
    val header =List[String](
    "timestamp","elb","client:port","backend:port","request_processing_time",
    "backend_processing_time","response_processing_time","elb_status_code",
    "backend_status_code","received_bytes","sent_bytes","request","user_agent",
    "ssl_cipher","ssl_protocol"
    )
    
    val timeFormat = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")
    val threshold = 35*60    
    def parser(x:String) : Map[String,String] = {
        val tempVar = x.split(" \"|\" ")
        // Split the String on "\"" to get 
        val retValues = tempVar(0).split(" ") ++ tempVar.slice(1,3) ++ tempVar(3).split(" ")
        val ret = (this.header zip retValues).toMap
        return ret
    }
    
    def getDeltas(x:Array[Map[String,String]]) : Array[Double] = {
        var out = Array[Double]()
        for (id <- 0 to x.length-2) {
            out = out :+ (this.timeFormat.parse(x(id + 1)("timestamp")).getTime() 
                - this.timeFormat.parse(x(id)("timestamp")).getTime())/1000.0
        }
        return out
    }
    
    def splitOnSessionDelta(ip:String,groupedMaps:Array[Map[String,String]]) : 
    List[(String,Array[Map[String,String]])]= {
        val deltas = this.getDeltas(groupedMaps)
        var idToSplit= Array[Int]()
        for((d,i) <- deltas.zipWithIndex if d >= this.threshold)
            idToSplit = idToSplit :+ i
        var out = List[(String,Array[Map[String,String]])]()
        if (idToSplit.length > 0) {
            var start = 0
            var end = 0
            for (i <- idToSplit){
                end = i+1
                out = out :+ (ip,groupedMaps.slice(start,end))
                start = i+1
            }
            out = out :+ (ip,groupedMaps.slice(idToSplit.last+1,groupedMaps.length))
        } else{ 
            out = List((ip,groupedMaps))
        }
        return out
    }
}

object FeatureExtraction extends java.io.Serializable {
    def getPath(url:String): String = {
        var pattern = """\.[a-zA-Z0-9]+:?[0-9]*(\/[a-zA-Z0-9]+)\/""".r
        var elems = pattern.findFirstMatchIn(url)
        if (elems == None ) {
            pattern = """\.[a-zA-Z0-9]+:?[0-9]*(\/[a-zA-Z0-9]*)""".r
            elems = pattern.findFirstMatchIn(url)
        }
        return (if (elems !=  None) elems.get.group(1) else "/")
    }
    // '''
    // getSessionTime -  method to get time duration of a particular session
    // '''
    def getSessionTime(groupedMaps:Array[Map[String,String]]): Double = {
        var ret = 0.0
        if (groupedMaps.length > 1) {

            ret = (LogParser.timeFormat.parse(groupedMaps.last("timestamp")).getTime() - 
            LogParser.timeFormat.parse(groupedMaps(0)("timestamp")).getTime())/1000
        }
        return (ret + math.max(0.0,groupedMaps.last("request_processing_time").toDouble) + 
        math.max(0.0,(groupedMaps.last("backend_processing_time").toDouble)) +
        math.max(0.0,(groupedMaps.last("response_processing_time").toDouble)))
    }
    // '''
    // countKPIs  - method to count number of occurances of each element of y 
    //              in x
    // '''
    def countKPIs(x:Array[String],y:Array[String],name:String): Array[(String,Double)] = {
        var ret = Array[(String,Double)]()
        for (i <- y) ret = ret :+ (name+"_"+i.replace('.','*'),x.count(_ == i)*1.0)
        return ret
    }
    // '''
    // createKPIs  - method to create all features
    // '''
    val elbstatus = Array("1","2","3","4","5")

    val hostlist = Array("/favicon", "/api", "", "/papi", "/offer", "/shop")

    def createKPIs(groupedMaps:Array[Map[String,String]],formapper:Boolean = true) : Map[String,Double]={
        // Session time
        val sessiontime:(String,Double) = ("sessiontime",getSessionTime(groupedMaps))
        // Unique URLs
        val numurls:(String,Double) = ("numurls",groupedMaps.map(i=> i("request") ).distinct.length)
        // Time per url
        val timeperurl:(String,Double) = ("timeperurl",sessiontime._2/numurls._2)
        val avgdeltatimes:(String,Double) = ("avgdeltatimes",LogParser.getDeltas(groupedMaps).sum/groupedMaps.length)
        val avg_request_processing_time :(String,Double) =  ("avg_request_processing_time_per_url",
            groupedMaps.map(i=> math.max(0.0,i("request_processing_time").toDouble)).sum/numurls._2)
        val avg_backend_processing_time :(String,Double) =  ("avg_backend_processing_time_per_url",
            groupedMaps.map(i=> math.max(0.0,i("backend_processing_time").toDouble)).sum/numurls._2)
        val avg_response_processing_time :(String,Double) =  ("avg_response_processing_time_per_url",
            groupedMaps.map(i=> math.max(0.0,i("response_processing_time").toDouble)).sum/numurls._2)
        val total_received_bytes :(String,Double) = 
        ("total_received_bytes_per_url",
            groupedMaps.map(i=> i("received_bytes").toDouble).sum/numurls._2)
        val total_sent_bytes :(String,Double) = 
        ("total_sent_bytes_per_url",
            groupedMaps.map(i=> i("sent_bytes").toDouble).sum/numurls._2)
        val elbstatuscode:Array[(String,Double)] = countKPIs(groupedMaps.map(i=> i("elb_status_code")),
            elbstatus,"elb_status_code")    
        val request :Array[(String,Double)] = countKPIs(
            groupedMaps.map(i=> getPath(i("request"))),hostlist,"request")
        var ret = Array[(String,Double)]()
        ret = ret :+ sessiontime :+
            numurls :+
            avg_response_processing_time :+
            avg_backend_processing_time :+ 
            avg_request_processing_time :+
            timeperurl :+
            avgdeltatimes :+
            total_received_bytes :+
            total_sent_bytes
        ret = ret ++
            request ++ 
            elbstatuscode
        return ret.toMap
    }
}

object analysis {
    def main(args: Array[String]) {
        val dataHome = "/home/sanjay/Kidagiri/algorithm_ds/paytm/WeblogChallenge/data/"
        val dataPath = dataHome + "*.log"
        val rawRDD = Spark.sc.textFile(dataPath).map(LogParser.parser).cache()
        val ipGroupedAnalysisRDD = rawRDD.map(
            x => {(x("client:port").split(':')(0) , List(x))}
            ).reduceByKey(
             _ ::: _
            ).map(
                x => {
                    (
                        x._1,
                        scala.util.Sorting.stableSort(x._2,
                        (e1: Map[String,String], e2: Map[String,String]) => 
                        e1("timestamp") < e2("timestamp"))
                    )
                }
            )
        val sessionAnalysisRDD = ipGroupedAnalysisRDD.flatMap( x =>
            LogParser.splitOnSessionDelta(x._1,x._2)
            )

        val temprecord = sessionAnalysisRDD.take(1)(0)._2
        val KPIs = sessionAnalysisRDD.map(i => FeatureExtraction.createKPIs(i._2))
        val LowSessionTimeUsers = KPIs.filter(i=> i("sessiontime")  < 350)
        val HighSessionTimeUsers = KPIs.filter(i=> i("sessiontime")  >= 350)
    }
}
