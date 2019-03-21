val data = sc.textFile("/FileStore/tables/soc_LiveJournal1Adj-2d179.txt")

var lines = data.map(x=>x.split("\t")).filter(x=>x.length==2).map(x=> (x(0),x(1).split(",")))

var friends = lines.flatMap(x=>x._2.map(a=>(
      if(x._1.toInt <= a.toInt)
      {
        (x._1.toInt,a.toInt)-> x._2
      }
      else
      {
        (a.toInt,x._1.toInt)-> x._2
      }
    )))

var mutualCount = friends.reduceByKey((a,b)=>a intersect(b)).map(a=> (a._1._1,a._1._2,a._2.length))

def output(args : (Int, Int, Int)) = {
  println(args._1+","+args._2 +" "+ args._3)
}

mutualCount.collect.foreach(output)


