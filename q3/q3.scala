var businessData = sc.textFile("/FileStore/tables/business.csv")
var reviewData = sc.textFile("/FileStore/tables/review.csv")

var businessSplit = businessData.map(x => x.split("::"))
var reviewSplit = reviewData.map(x => x.split("::"))

var collegeBusiness = businessSplit.filter(x => x(2).contains("Colleges & Universities")).map(x=> (x(0),(x(1), x(2)))) 

var collegeReview = reviewSplit.map(x=> (x(2), (x(0), x(1), x(3)))).join(collegeBusiness)

var userRatings = collegeReview.map(x=> (x._2._1._2, x._2._1._3)).sortByKey()

def output(args : (String, String)) = {
  println(args._1+"\t\t"+ args._2 )
}

userRatings.collect.foreach(output)

userRatings.distinct.collect.foreach(output)

