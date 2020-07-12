package bigdata;

import org.apache.spark.{SparkConf, SparkContext}

object Task4 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Task4");
    val sc = new SparkContext(conf);

    val input1 = args(0);
    val input2 = args(1);
    val output = args(2);

    val txt1 = sc.textFile(input1);
    val txt2 = sc.textFile(input2);

    def factorial(n:Int): Int = if(n==0) 1 else n*factorial(n-1);

    val map1 =txt1.map{x=>(x.split("\t")(0),x.split("\t")(1))};
    val map2 =txt2.map{x=>(x.split("\t")(0),x.split("\t")(1))};
    val tjoin=map1.join(map2).map{case(x,(y,z))=>(x,z.toFloat/((y.toFloat*(y.toFloat-1))/2))};
    val res=tjoin.map(x=>x._1+"\t"+x._2).saveAsTextFile(output);

  }
}
