

class tr2LiteMatCHAIN {
  import org.apache.spark.sql.SaveMode
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.{SparkConf, SparkContext}
  import org.apache.spark.graphx.{Graph, Edge}
  import org.apache.spark.HashPartitioner
  import java.util.{Calendar, Locale}

  val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]").set("spark.executor.instances", "8").set("spark.default.parallelism","8")
  val spark = SparkSession.builder.config(sparkConf).getOrCreate()
  val sc = spark.sparkContext

  //===============================================================================

  sc.setLogLevel("ERROR")

  //new SparkConf().set("spark.executor.instances", "8").set("spark.default.parallelism","8")

  import spark.implicits._

  val start = System.currentTimeMillis()

  val factsPath = "hdfs://small1.common.lip6.fr:54310/user/olivier/lascar/chain/lubm10000_depart20to25_depth10to20_branches1to1"

  val factsFileExt =".nt"
  val prop = "<http://swat.cse.lehigh.edu/onto/univ-bench.owl#subOrganizationOf>"
  val propId = propDictionary.loopUp(prop)

  val facts = sc.textFile(factsPath+factsFileExt).map(line => line.split(" ")).map(x => (x(2), propId, x(0)))

  val triplesCount = facts.count()

  val factsEntities = facts.map(x => (x._1, x._3)).flatMap { case (s, o) => Array(s, o) }.distinct().zipWithUniqueId()
  val nodes = factsEntities.map(x => (x._2, x._1))
  nodes.persist()
  val nodesCount = nodes.count

  val facts2 = facts.map(x => (x._1, (x._3, x._2))).join(factsEntities).map { case (k, ((o, p), sid)) => (o, (sid, p)) }

  val endLoading = System.currentTimeMillis()

  println("*********************************************")
  println(s"*  Triples count       = $triplesCount")
  println(s"*  Dictionary size     = $nodesCount")
  println("*  Data loading + naive dictionary : "+ (endLoading - start)+ " ms")
  println("*********************************************")

  val tmp2 = sc.textFile(factsPath+"_CC").map(x=>x.split(" ")).map(x=>(x(0).toLong,(x(1).toLong,x(2).toLong)))
  val tmp3 = tmp2.groupByKey()

  def lenMoreOne(l1 : Iterable[(Long,Long)]) : Boolean = {
    val l2 = l1.map(x=>(x._2,x._1))
    for(e1 <- l1)
      for(e2<- l2)
        if(e1._1==e2._1)
          return true
    false
  }

  val le =tmp3.partitionBy(new HashPartitioner(36))

  le.persist()
  println("*********************************************")
  println("*  Number of CCs with a length > 1   = " + le.count())

  val outDeg = le.flatMap(x=>x._2.map(y=>((x._1,y._1),1))).reduceByKey(_+_)

  val inDeg = le.flatMap(x=>x._2.map(y=>((x._1,y._2),1))).reduceByKey(_+_)

  val inOutDeg = inDeg.fullOuterJoin(outDeg)
  val inOutDeg2 = inOutDeg.map{case((k,n),(in,out))=>(k, (n, in match {case Some(x) => x; case None => 0 }, out match {case Some(x)=>x; case None => 0}))}.groupByKey()
  inOutDeg2.count()

  println("Chains ===========================")
  def isSinglyRootedChain(l : Iterable[(Long,Int,Int)]) : Boolean = {
    var rootsCount = 0
    for(e <- l) {
      if (e._2 > 1 || e._3 > 1)
        return false
      else if (e._2 == 0)
        rootsCount = rootsCount + 1
    }
    if(rootsCount==1)
      return true
    false
  }

  val chains = inOutDeg2.filter(x=>isSinglyRootedChain(x._2))

  println("count chains : "+chains.count())

  def getRoot(l : Iterable[(Long,Int,Int)]) : Option[Long] = {
    for(e <- l)
      if(e._2==0)
        return Some(e._1)
    None
  }

  val chainsWithRoot = chains.map(x=>(x._1,getRoot(x._2))).filter(x=>x._2!= None).map(x=>(x._1,x._2 match{case Some(x) => x}))
  val chainsWithRootCCs = le.join(chainsWithRoot)
  le.unpersist()

  import scala.collection.mutable.ListBuffer
  def encodeChain(l : Iterable[(Long,Long)], node : Long, cpt : Int, dict : List[(Long, Int)]) : List[(Long, Int)] = {
    if(l.size>0) {
      val dst = l.filter(x => x._1 == node).toList.head
      val dictEntry = List((node, cpt))
      encodeChain(l.filter(x => x._1 != node), dst._2, cpt + 1, dict ++ dictEntry)
    } else {
      val dictEntry = List((node, cpt))
      dict ++ dictEntry
    }
  }

  val chainsWithDict =chainsWithRootCCs.map(x=>(prop,x._1,encodeChain(x._2._1,x._2._2,1, List[(Long,Int)]())))

  val endChain = System.currentTimeMillis()

  println("*  Chain encoding computation : "+ (endChain - endLoading)+ " ms")
  println("*********************************************")
  val flatChainsWithDict = chainsWithDict.flatMap(x=>x._3.map(y=>(y._1,(840L,x._2,y._2))))
  val dict = nodes.join(flatChainsWithDict).map{case(id,(uri,(pid,ccid,lid)))=>(uri,pid,ccid,lid)}
  dict.map(x=>x._1+" "+x._2+" "+x._3+" "+x._4).saveAsTextFile(factsPath+"_dictLiteMat_"+endChain)

  //////////////////////////////////////////
  //// MAT SECTION
  //////////////////////////////////////////

  val t4 = System.currentTimeMillis()

  import scala.collection.mutable.ListBuffer

  def matChain(l: Iterable[(Long, Long)], node: Long, dict: List[(Long, Long)]): List[(Long, Long)] = {
    if (l.size > 0) {
      val src = l.filter(x => x._1 == node).toList.head
      val dsts = l.filter(x => x._1 != node).toList
      val entries = for (dst <- dsts) yield (src._1, dst._2)
      matChain(l.filter(x => x._1 != node), src._2, dict ++ entries)
    } else {
      dict
    }
  }

  val matChains = chainsWithRootCCs.map(x => (840L,matChain(x._2._1, x._2._2, List[(Long, Long)]())))

  val endMat = System.currentTimeMillis()

  val matFacts = matChains.flatMap(x=>x._2.map(y=>(y._1,840L,y._2)))
  matFacts.map(x => x._1 + " " + x._2 + " " + x._3).saveAsTextFile(factsPath + "_FullMat_" + endChain)
  println("*  # materialized triples  = "+matFacts.count)
  println("*  Chain materialization computation : " + (endMat - t4) + " ms")
  println("*********************************************")
  nodes.map(x => x._2 + " " + x._1).saveAsTextFile(factsPath + "_dictFullMat_" + endChain)

}
