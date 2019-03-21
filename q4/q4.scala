var businessData = sc.textFile("/FileStore/tables/business.csv")
var reviewData = sc.textFile("/FileStore/tables/review.csv")

var businessSplit = businessData.map(x => x.split("::"))
var reviewSplit = reviewData.map(x => x.split("::"))


var businessMap = businessSplit.filter(x=> x(1).contains("NY")).map(x => (x(0),(x(1),x(2)))).reduceByKey( (a,b) => a)


var reviewMap = reviewSplit.map(x => (x(2),(x(3).toDouble,1.toDouble))).reduceByKey((x,y)=> (x._1+y._1, x._2+y._2)).mapValues{case(x:Double,y:Double) => x.toDouble/y.toDouble}.map(a => (a._1,a._2))

var reviewBusiness = reviewMap.join(businessMap)

var sortMap = reviewBusiness.map(x=> ((x._2._1),(x._1, x._2._2._1, x._2._2._2))).sortByKey(false).top(10)


var result = sortMap.map(x=> (x._2._1,x._2._2,x._2._3,x._1))

def output(args : (String, String, String, Double)) = {
  println("BusinessId: "+args._1+" , Address : "+ args._2 +" , Categories : "+args._3+" , Average Rating : "+args._4 )
}


result.foreach(output)

