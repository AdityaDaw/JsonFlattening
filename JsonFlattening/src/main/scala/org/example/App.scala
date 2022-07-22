package org.example

import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col,explode_outer}
import scala.annotation.tailrec

/**
 * Hello world!
 *
 */
object App{
  def main(args: Array[String]): Unit = {
    val spark =SparkSession.builder()
      .master("local")
      .appName("json_flattening")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.caseSensitive","true")
    val df = spark.read.json("/home/aditya/Desktop/Projects/JsonFlattening/src/main/resources/test.json")
    df.show()

    @tailrec
    def structFlattener(df:DataFrame, separator_string:String):DataFrame={
      val fields = df.schema.fields
      val struct_list = fields.filter(field => field.dataType.isInstanceOf[StructType]).map(struct_field => struct_field.name).toList
      if (struct_list.isEmpty) df else {
        var newdf = df
        struct_list.foreach{field_name =>
          val struct_name = field_name + ".*"
          val nameList = newdf.select(struct_name).columns.toList
          val struct_name_list = nameList.map(f => field_name + "." + f + " as " + field_name + separator_string + f)
          val all_column_list = newdf.columns.toList.filter(x => !x.contentEquals(field_name)) ::: struct_name_list
          newdf = newdf.selectExpr(all_column_list:_*)
        }
        structFlattener(newdf,separator_string)
      }
    }

    @tailrec
    def arrayFlattener(df: DataFrame):DataFrame={
      val fields = df.schema.fields
      val array_list : List[String] = fields.filter(field => field.dataType.isInstanceOf[ArrayType]).map(f => f.name).toList
      if (array_list.isEmpty) df else {
        var newdf = df
        val col_string = array_list.mkString(",")
        val array_zip_string =  "arrays_zip(" + col_string + ") as result"
        newdf = newdf.selectExpr("*",array_zip_string)
        newdf = newdf.select(newdf.columns.filter(colName => !array_list.contains(colName)).map(colName => new Column(colName)): _*)
        newdf = newdf.withColumn("result_ex",explode_outer(col("result"))).drop("result")
        newdf = newdf.select("*","result_ex.*").drop("result_ex")
        arrayFlattener(newdf)
      }
    }

    def flatteningFunction(df : DataFrame, separetor_string : String):DataFrame={
      val fields = df.schema.fields
      val complex_element_list = fields.filter(field => field.dataType.isInstanceOf[ArrayType] || field.dataType.isInstanceOf[StructType])
      if (complex_element_list.isEmpty) df else {
        val struct_free_df = structFlattener(df, separetor_string)
        val array_free_df = arrayFlattener(struct_free_df)
        flatteningFunction(array_free_df,separetor_string)
      }
    }

    val result_df =flatteningFunction(df,"__")
    result_df.show()
    result_df.printSchema()

  }
}
