package overview

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import scala.io.Source

object overview {

case class schema(
		txnno:String,
		txndate:String,
		custno:String,
		amount:String,
		category:String,
		product:String,
		city:String,
		state:String,
		spendby:String
		)


def main(args:Array[String]):Unit={

		System.setProperty("hadoop.home.dir", "E:\\hadoop") 
		println("====started==")

		val conf = new SparkConf().setAppName("first").setMaster("local[*]")
		.set("spark.driver.allowMultipleContexts","true")


		val sc = new SparkContext(conf)
		sc.setLogLevel("ERROR")


		val spark = SparkSession.builder().getOrCreate()
		import spark.implicits._


		val listcol= List("txnno","txndate","custno","amount","category","product","city","state","spendby")


		val data = sc.textFile("file:///E:/data/revdata/file1.txt")

		data.take(5).foreach(println)


		println
		println("==============Gymnastics rows============")
		println


		val gymdata = data.filter( x => x.contains("Gymnastics"))

		gymdata.take(5).foreach(println)


		val mapsplit = gymdata.map( x => x.split(","))

		val schemardd = mapsplit.map( x => schema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))

		val prodfilter = schemardd.filter( x => x.product.contains("Gymnastics"))

		println
		println("==============prod column filter============")
		println

		prodfilter.take(5).foreach(println)


		println
		println("==============schema rdd to dataframe============")
		println		


		val schemadf = prodfilter.toDF().select(listcol.map(col): _*)

		schemadf.show(5)




		val file2 = sc.textFile("file:///E:/data/revdata/file2.txt")

		val mapsplit1 = file2.map( x => x.split(","))

		val rowrdd = mapsplit1.map( x => Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))

		println
		println("==============Row rdd============")
		println		


		rowrdd.take(5).foreach(println)


		val rowschema = StructType(Array(
				StructField("txnno",StringType,true),
				StructField("txndate",StringType,true),
				StructField("custno",StringType,true),
				StructField("amount", StringType, true),
				StructField("category", StringType, true),
				StructField("product", StringType, true),
				StructField("city", StringType, true),
				StructField("state", StringType, true),
				StructField("spendby", StringType, true)
				))



		val rowdf = spark.createDataFrame(rowrdd, rowschema).select(listcol.map(col): _*)		

		println
		println("==============Row df============")
		println		


		rowdf.show(5)		


		val csvdf = spark.read.format("csv").option("header","true")
		.load("file:///E:/data/revdata/file3.txt").select(listcol.map(col): _*)


		println
		println("==============csv df============")
		println		


		csvdf.show(5)				


		val jsondf = spark.read.format("json")
		.load("file:///E:/data/revdata/file4.json").select(listcol.map(col): _*)

		println
		println("==============jsondf============")
		println		


		jsondf.show(5)			


		println
		println("==============parquetdf============")
		println		



		val parquetdf = spark.read.load("file:///E:/data/revdata/file5.parquet").select(listcol.map(col): _*)

		parquetdf.show(5)




		val xmldf = spark.read.format("xml").option("rowtag","txndata")  
		.load("file:///E:/data/revdata/file6").select(listcol.map(col): _*)


		println
		println("==============xmldf============")
		println		


		xmldf.show(5)

		println
		println("==============uniondf============")
		println			


		val uniondf = schemadf.union(rowdf).union(csvdf).union(jsondf).union(parquetdf).union(xmldf)


		uniondf.show(5)


		println
		println("==============proc df============")
		println					


		val procdf = uniondf.withColumn("txndate", expr("split(txndate,'-')[2]"))
		.withColumnRenamed("txndate","year")
		.withColumn("status",expr("case when spendby='cash' then 1 else 0 end"))
		.filter(col("txnno")>50000)

		procdf.show(5)

		println
		println("==============agg df============")
		println			


		val aggdf = procdf.groupBy("category").agg(sum("amount").cast(IntegerType).as("total"))

		aggdf.show(5)



		uniondf
		.write
		.format("avro")
		.mode("append")
		.partitionBy("category")
		.save("file:///E:/data/revavrodata")




		val cust=	spark.read.format("csv")
		.option("header","true")
		.load("file:///E:/data/revdata/cust.csv")

		cust.show()


		val prod=	spark.read.format("csv").option("header","true")
		.load("file:///E:/data/revdata/prod.csv")	


		prod.show()


		println
		println("==============inner df============")
		println				


		val inner = cust.join(prod,Seq("id"),"inner")

		inner.show()


		println
		println("==============left df============")
		println				


		val left = cust.join(prod,Seq("id"),"left")

		left.show()		


		println
		println("==============right df============")
		println				


		val right = cust.join(prod,Seq("id"),"right")

		right.show()		


		println
		println("==============full df============")
		println				


		val full = cust.join(prod,Seq("id"),"full")

		full.show()	


		println
		println("==============anti df============")
		println				


		val anti = cust.join(prod,Seq("id"),"left_anti")

		anti.show()				


}

}
