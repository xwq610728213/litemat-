class tr2LiteMatTREE {
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.{SparkConf, SparkContext}
  import org.apache.spark.graphx.{Graph, Edge}
  import java.util.{Calendar, Locale}
  import org.apache.spark.HashPartitioner

  val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
  val spark = SparkSession.builder.config(sparkConf).getOrCreate()
  val sc = spark.sparkContext

  sc.setLogLevel("ERROR")

  import spark.implicits._

  val start = System.currentTimeMillis()

  val factsPath = "hdfs://small1.common.lip6.fr:54310/user/olivier/lascar/tree/lubm10000_depart20to25_depth10to20_branches1to5"

  val factsFileExt =".nt"
  val prop = "<http://swat.cse.lehigh.edu/onto/univ-bench.owl#subOrganizationOf>"
  val propId = propDictionary.loopUp(prop)

  val facts = sc.textFile(factsPath+factsFileExt).map(line => line.split(" ")).map(x => (x(2), 840L, x(0)))
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

  def isSinglyRootedChain(l: Iterable[(Long, Int, Int)]): Boolean = {
    var rootsCount = 0
    for (e <- l) {
      if (e._2 > 1 || e._3 > 1)
        return false
      else if (e._2 == 0)
        rootsCount = rootsCount + 1
    }
    if (rootsCount == 1)
      return true
    false
  }

  val chains = inOutDeg2.filter(x => isSinglyRootedChain(x._2))

  println("count chains : " + chains.count())

  def getRoot(l: Iterable[(Long, Int, Int)]): Option[Long] = {
    for (e <- l)
      if (e._2 == 0)
        return Some(e._1)
    None
  }

  def isDag(l: Iterable[(Long, Int, Int)]): Boolean = {
    for (e <- l)
      if (e._2 > 1)
        return true
    false
  }

  val nonChains = inOutDeg2.subtract(chains).filter(x => !isDag(x._2))
  val nonChains2 = nonChains.map(x => (x._1, x._2, getRoot(x._2))).filter(x => x._3 != None).map(x => (x._1, (x._2, x._3 match {
    case Some(x) => x
  })))
  val nonChainsWithRootCCs = le.join(nonChains2)

  def dfsMutableTreeEncoding(edg: Iterable[(Long, Long)], node: Long, cpt: Long): List[(Long, Long, Int)] = {
    val tmpDict: scala.collection.mutable.Set[(Long, Long)] = scala.collection.mutable.Set[(Long, Long)]((node, cpt))

    def loop(edg: Iterable[(Long, Long)], node: Long, cpt: Long): Unit = {
      if (!edg.isEmpty) {
        val dsts = edg.filter(x => x._1 == node).toList
        if (dsts.size > 0) {
          val nbBits = Math.ceil(Math.log(1+ dsts.size) / Math.log(2)).toInt
          var localCpt = 0
          val newCpt = cpt << nbBits.toInt
          for (dst <- dsts) {
            localCpt = localCpt + 1
            val dictEntry = (dst._2, (newCpt + localCpt).toLong)
            tmpDict += dictEntry
            loop(edg.filter(x => x._1 != node || x._2 != dst._2), dst._2, newCpt + localCpt)
          }
        }
      }
    }

    loop(edg, node, cpt)
    val maxEncodedValue = tmpDict.map(x => x._2).reduce(Math.max(_,_))
    val maxEncodBitLength = Math.ceil(Math.log(maxEncodedValue) / Math.log(2)).toLong

    tmpDict.map(x => (x._1, x._2 match {
      case 1 => x._2 << (maxEncodBitLength -1).toInt
      case _ => x._2 << (maxEncodBitLength - (Math.ceil(Math.log(x._2) / Math.log(2)).toInt))
    }, x._2 match {
      case 1 => 1
      case _ => (Math.ceil(Math.log(x._2) / Math.log(2))).toInt
    } )).toList
  }

  val treesWithDict = nonChainsWithRootCCs.map(x => (prop, x._1, dfsMutableTreeEncoding(x._2._1, x._2._2._2, 1L)))

  val endChain = System.currentTimeMillis()
  println("*  Tree encoding computation : "+ (endChain - endLoading)+ " ms")
  println("*********************************************")


  val flatTreesWithDict = treesWithDict.flatMap(x=>x._3.map(y=>(y._1.toLong,(840L,x._2,y._2))))
  flatTreesWithDict.count
  val dict = nodes.join(flatTreesWithDict).map{case(id,(uri,(pid,ccid,lid)))=>(uri,pid,ccid,lid)}
  dict.map(x=>x._1+" "+x._2+" "+x._3+" "+x._4).saveAsTextFile(factsPath+"_dictLiteMat_"+endChain)
  //////////////////////////////////////////
  //// MAT SECTION
  //////////////////////////////////////////
  val startMat = System.currentTimeMillis()

  def matTree(l: Iterable[(Long, Long, Int)], node: Long, maxEncodBitLength: Int): List[(Long, Long)] = {
    var mat: scala.collection.mutable.Set[(Long, Long)] = scala.collection.mutable.Set[(Long,Long)]()

    def loop(l: Iterable[(Long, Long,Int)], node: Long) : Unit = {
      if (l.size > 1) {
        val srcList = l.filter(x => x._1 == node).toList
        val src = srcList.head
        val upperBound = ((src._2 >> (maxEncodBitLength - src._3)) + 1) << (maxEncodBitLength - src._3)
        val dsts = l.filter(x => x._1 != node).toList
        val tmpMat = for (dst <- dsts if (dst._2 > src._2 && dst._2 < upperBound)) yield (src._1, dst._1)
        mat = mat ++ tmpMat
        val minCode = dsts.map(x=>x._2).reduce(Math.min(_,_)).toLong
        val minVal = dsts.filter(x=>x._2==minCode).toList.head
        loop(dsts, minVal._1)
      }
    }

    loop(l,node)
    mat.toList
  }

  val matTrees = treesWithDict.flatMap(x => matTree(x._3, x._2, Math.ceil(Math.log(x._3.map(x => x._2).reduce(Math.max(_,_))) / Math.log(2)).toInt))
  matTrees.count()

  val endMat = System.currentTimeMillis()
  println("*  Tree materialization computation : "+ (endMat - startMat)+ " ms")
  println("*********************************************")

  matTrees.map(x=>x._1+" 840 "+x._2).saveAsTextFile(factsPath+"_FullMat_"+endChain)
  nodes.map(x=>x._1+" "+x._2).saveAsTextFile(factsPath+"_dictFullMat_"+endChain)
}

