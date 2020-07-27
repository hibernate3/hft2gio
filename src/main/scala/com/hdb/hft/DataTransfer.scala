package com.hdb.hft

import com.alibaba.fastjson.JSONObject
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import java.io.FileWriter

object DataTransfer {

  val debug: Boolean = false

  case class BaseEvent(user_id: String, event_time: String)
  case class SyChooseCity(user_id: String, event_time: String, element_content: String)
  case class LpListClick(user_id: String, event_time: String, element_content: String)
  case class SearchResultClick(user_id: String, event_time: String, element_content: String)
  case class ZxBannerClick(user_id: String, event_time: String, url: String)
  case class ZxListClick(user_id: String, event_time: String, element_content: String)
  case class ZxDetailPageView(user_id: String, event_time: String, url: String)
  case class LpDetailPageView(user_id: String, event_time: String, url: String)
  case class FxPosterClick(user_id: String, event_time: String, url: String)
  case class LxPhoneClick(user_id: String, event_time: String, element_target_url: String)
  case class LxCustomerClick(user_id: String, event_time: String, url: String)

  val nameNode: String = "10.71.81.220"

  val numPartitions: Int = 5;

  def main(args: Array[String]): Unit = {
    val sqlContext = SparkSession.builder().appName(this.getClass.getSimpleName).enableHiveSupport().getOrCreate()
    sqlContext.sparkContext.setLogLevel("WARN")

    //    sqlContext.sql("select * from buried_point.ods_ext_hdb_buried_data").show()

    executeSyPageView(sqlContext)
    executeSyChooseCity(sqlContext)
    executeLpListClick(sqlContext)
    executeSearchResultPageView(sqlContext)
    executeSearchResultClick(sqlContext)
    executeZxBannerClick(sqlContext)
    executeZxListPageView(sqlContext)
    executeZxListClick(sqlContext)
    executeZxDetailPageView(sqlContext)
    executeLpDetailPageView(sqlContext)
    executeFxPosterClick(sqlContext)
    executeLxPhoneClick(sqlContext)
    executeLxCustomerClick(sqlContext)
    executeRegisterPageView(sqlContext)
  }

  def executeSyPageView(sparkSession: SparkSession): Unit = {
    var sql:String = ""
    if (debug) {
      sql = "select * from buried_point.ods_ext_hdb_buried_data"
    } else {
      sql = "select * from buried_point.ods_ext_hdb_buried_data where length(user_id)>10 and event='AppClick' and element_content='首页' and element_type='TextView' and createday >= '2020-01-01'"
    }

    import sparkSession.implicits._
    var dataRdd: RDD[BaseEvent] = sparkSession.sql(sql).as[BaseEvent].rdd

    val jsonRdd: RDD[String] = dataRdd.map(item => {
      try {
        val timestamp: String = item.event_time
        val userId: String = item.user_id
        val event: String = "hft_syPageView"

        val jsonObj = new JSONObject()
        if (debug) {
          jsonObj.put("timestamp", timestamp)
        } else {
          jsonObj.put("timestamp", timestamp.toLong)
        }
        jsonObj.put("userId", userId)
        jsonObj.put("event", event)
        jsonObj.toJSONString
      } catch {
        case ex: Exception => {
          ""
        }
      }
    })

//    jsonRdd.coalesce(10, true).saveAsTextFile("hdfs://bigdata-prd-nn-02:8020/temp/wangyuhang/syPageView_0")
    jsonRdd.coalesce(numPartitions, true).saveAsTextFile("hdfs://" + nameNode + ":8020/temp/wangyuhang/hft/syPageView")
  }

  def executeSyChooseCity(sparkSession: SparkSession): Unit ={
    var sql = ""
    if (debug) {
      sql = "select * from buried_point.ods_ext_hdb_buried_data"
    } else {
      sql = "select * from buried_point.ods_ext_hdb_buried_data where length(user_id)>10 and event='AppClick' and element_id='city' and element_type='TextView' and createday >= '2020-01-01'"
    }

    import sparkSession.implicits._
    var dataRdd: RDD[SyChooseCity] = sparkSession.sql(sql).as[SyChooseCity].rdd

    val jsonRdd: RDD[String] = dataRdd.map(item => {
      try {
        val timestamp: String = item.event_time
        val userId: String = item.user_id
        val event: String = "hft_syChooseCity"
        val cityName_var: String = item.element_content

        val jsonObj = new JSONObject()
        if (debug) {
          jsonObj.put("timestamp", timestamp)
        } else {
          jsonObj.put("timestamp", timestamp.toLong)
        }
        jsonObj.put("userId", userId)
        jsonObj.put("event", event)

        val attrsObj = new JSONObject()
        attrsObj.put("cityName_var", cityName_var)
        jsonObj.put("attrs", attrsObj)

        jsonObj.toJSONString
      } catch {
        case ex: Exception => {
          ""
        }
      }
    })

    jsonRdd.coalesce(numPartitions, true).saveAsTextFile("hdfs://" + nameNode + ":8020/temp/wangyuhang/hft/syChooseCity")
  }

  def executeLpListClick(sparkSession: SparkSession): Unit ={
    var sql = "select * from buried_point.ods_ext_hdb_buried_data where length(user_id)>10 and element_content rlike '^[\u4e00-\u9fa5].*[-]{1}\\d{8}$' and createday >= '2020-01-01'"

    import sparkSession.implicits._
    var dataRdd: RDD[LpListClick] = sparkSession.sql(sql).as[LpListClick].rdd

    val jsonRdd: RDD[String] = dataRdd.map(item => {
      try {
        val timestamp: String = item.event_time
        val userId: String = item.user_id
        val event: String = "hft_lpListClick"
        val lpName_var: String = item.element_content

        val jsonObj = new JSONObject()
        jsonObj.put("timestamp", timestamp.toLong)
        jsonObj.put("userId", userId)
        jsonObj.put("event", event)

        val attrsObj = new JSONObject()
        attrsObj.put("lpName_var", lpName_var)
        jsonObj.put("attrs", attrsObj)

        jsonObj.toJSONString
      } catch {
        case ex: Exception => {
          ""
        }
      }
    })

    jsonRdd.coalesce(numPartitions, true).saveAsTextFile("hdfs://" + nameNode + ":8020/temp/wangyuhang/hft/lpListClick")
  }

  def executeSearchResultPageView(sparkSession: SparkSession): Unit ={
    var sql = "select * from buried_point.ods_ext_hdb_buried_data where length(user_id)>10 and event='Pageview' and title='搜索楼盘' and createday >= '2020-01-01'"

    import sparkSession.implicits._
    var dataRdd: RDD[BaseEvent] = sparkSession.sql(sql).as[BaseEvent].rdd

    val jsonRdd: RDD[String] = dataRdd.map(item => {
      try {
        val timestamp: String = item.event_time
        val userId: String = item.user_id
        val event: String = "hft_searchResultPageView"

        val jsonObj = new JSONObject()
        jsonObj.put("timestamp", timestamp.toLong)
        jsonObj.put("userId", userId)
        jsonObj.put("event", event)

        jsonObj.toJSONString
      } catch {
        case ex: Exception => {
          ""
        }
      }
    })

    jsonRdd.coalesce(numPartitions, true).saveAsTextFile("hdfs://" + nameNode + ":8020/temp/wangyuhang/hft/searchResultPageView")
  }

  def executeSearchResultClick(sparkSession: SparkSession): Unit ={
    var sql = "select * from buried_point.ods_ext_hdb_buried_data where length(user_id)>10 and event='WebClick' and title='搜索楼盘' and createday >= '2020-01-01'"

    import sparkSession.implicits._
    var dataRdd: RDD[SearchResultClick] = sparkSession.sql(sql).as[SearchResultClick].rdd

    val jsonRdd: RDD[String] = dataRdd.map(item => {
      try {
        val timestamp: String = item.event_time
        val userId: String = item.user_id
        val event: String = "hft_searchResultClick"
        val searchWords_var: String = item.element_content

        val jsonObj = new JSONObject()
        jsonObj.put("timestamp", timestamp.toLong)
        jsonObj.put("userId", userId)
        jsonObj.put("event", event)

        val attrsObj = new JSONObject()
        attrsObj.put("searchWords_var", searchWords_var)
        jsonObj.put("attrs", attrsObj)

        jsonObj.toJSONString
      } catch {
        case ex: Exception => {
          ""
        }
      }
    })

    jsonRdd.coalesce(numPartitions, true).saveAsTextFile("hdfs://" + nameNode + ":8020/temp/wangyuhang/hft/searchResultClick")
  }

  def executeZxBannerClick(sparkSession: SparkSession): Unit = {
    var sql = "select * from buried_point.ods_ext_hdb_buried_data where length(user_id)>10 and event='Pageview' and title='资讯详情' and createday >= '2020-01-01'"

    import sparkSession.implicits._
    var dataRdd: RDD[ZxBannerClick] = sparkSession.sql(sql).as[ZxBannerClick].rdd

    val jsonRdd: RDD[String] = dataRdd.map(item => {
      try {
        val timestamp: String = item.event_time
        val userId: String = item.user_id
        val event: String = "hft_zxBannerClick"
        val zxId_var: String = item.url.split("id=")(1)

        val jsonObj = new JSONObject()
        jsonObj.put("timestamp", timestamp.toLong)
        jsonObj.put("userId", userId)
        jsonObj.put("event", event)

        val attrsObj = new JSONObject()
        attrsObj.put("zxId_var", zxId_var)
        jsonObj.put("attrs", attrsObj)

        jsonObj.toJSONString
      } catch {
        case ex: Exception => {
          ""
        }
      }
    })

    jsonRdd.coalesce(numPartitions, true).saveAsTextFile("hdfs://" + nameNode + ":8020/temp/wangyuhang/hft/zxBannerClick")
  }

  def executeZxListPageView(sparkSession: SparkSession): Unit = {
    var sql = "select * from buried_point.ods_ext_hdb_buried_data where length(user_id)>10 and event='Pageview' and title='资讯列表' and createday >= '2020-01-01'"

    import sparkSession.implicits._
    var dataRdd: RDD[BaseEvent] = sparkSession.sql(sql).as[BaseEvent].rdd

    val jsonRdd: RDD[String] = dataRdd.map(item => {
      try {
        val timestamp: String = item.event_time
        val userId: String = item.user_id
        val event: String = "hft_zxListPageView"

        val jsonObj = new JSONObject()
        jsonObj.put("timestamp", timestamp.toLong)
        jsonObj.put("userId", userId)
        jsonObj.put("event", event)

        jsonObj.toJSONString
      } catch {
        case ex: Exception => {
          ""
        }
      }
    })

    jsonRdd.coalesce(numPartitions, true).saveAsTextFile("hdfs://" + nameNode + ":8020/temp/wangyuhang/hft/zxListPageView")
  }

  def executeZxListClick(sparkSession: SparkSession): Unit = {
    var sql = "select * from buried_point.ods_ext_hdb_buried_data where length(user_id)>10 and event='WebClick' and title='资讯详情' and createday >= '2020-01-01'"

    import sparkSession.implicits._
    var dataRdd: RDD[ZxListClick] = sparkSession.sql(sql).as[ZxListClick].rdd

    val jsonRdd: RDD[String] = dataRdd.map(item => {
      try {
        val timestamp: String = item.event_time
        val userId: String = item.user_id
        val event: String = "hft_zxListClick"
        val zxName_var: String = item.element_content

        val jsonObj = new JSONObject()
        jsonObj.put("timestamp", timestamp.toLong)
        jsonObj.put("userId", userId)
        jsonObj.put("event", event)

        val attrsObj = new JSONObject()
        attrsObj.put("zxName_var", zxName_var)
        jsonObj.put("attrs", attrsObj)

        jsonObj.toJSONString
      } catch {
        case ex: Exception => {
          ""
        }
      }
    })

    jsonRdd.coalesce(numPartitions, true).saveAsTextFile("hdfs://" + nameNode + ":8020/temp/wangyuhang/hft/zxListClick")
  }

  def executeZxDetailPageView(sparkSession: SparkSession): Unit = {
    var sql = "select * from buried_point.ods_ext_hdb_buried_data where length(user_id)>10 and event='Pageview' and title='资讯详情' and createday >= '2020-01-01'"

    import sparkSession.implicits._
    var dataRdd: RDD[ZxDetailPageView] = sparkSession.sql(sql).as[ZxDetailPageView].rdd

    val jsonRdd: RDD[String] = dataRdd.map(item => {
      try {
        val timestamp: String = item.event_time
        val userId: String = item.user_id
        val event: String = "hft_zxDetailPageView"
        val zxUrl_var: String = item.url

        val jsonObj = new JSONObject()
        jsonObj.put("timestamp", timestamp.toLong)
        jsonObj.put("userId", userId)
        jsonObj.put("event", event)

        val attrsObj = new JSONObject()
        attrsObj.put("zxUrl_var", zxUrl_var)
        jsonObj.put("attrs", attrsObj)

        jsonObj.toJSONString
      } catch {
        case ex: Exception => {
          ""
        }
      }
    })

    jsonRdd.coalesce(numPartitions, true).saveAsTextFile("hdfs://" + nameNode + ":8020/temp/wangyuhang/hft/zxDetailPageView")
  }

  def executeLpDetailPageView(sparkSession: SparkSession): Unit = {
    var sql = "select * from buried_point.ods_ext_hdb_buried_data where length(user_id)>10 and event='Pageview' and title='楼盘详情' and createday >= '2020-01-01'"

    import sparkSession.implicits._
    var dataRdd: RDD[LpDetailPageView] = sparkSession.sql(sql).as[LpDetailPageView].rdd

    val jsonRdd: RDD[String] = dataRdd.map(item => {
      try {
        val timestamp: String = item.event_time
        val userId: String = item.user_id
        val event: String = "hft_lpDetailPageView"
        val lpID_var: String = item.url.split("buildingId=")(1)

        val jsonObj = new JSONObject()
        jsonObj.put("timestamp", timestamp.toLong)
        jsonObj.put("userId", userId)
        jsonObj.put("event", event)

        val attrsObj = new JSONObject()
        attrsObj.put("lpID_var", lpID_var)
        jsonObj.put("attrs", attrsObj)

        jsonObj.toJSONString
      } catch {
        case ex: Exception => {
          ""
        }
      }
    })

    jsonRdd.coalesce(numPartitions, true).saveAsTextFile("hdfs://" + nameNode + ":8020/temp/wangyuhang/hft/lpDetailPageView")
  }

  def executeFxPosterClick(sparkSession: SparkSession): Unit = {
    var sql = "select * from buried_point.ods_ext_hdb_buried_data where length(user_id)>10 and event='Pageview' and title='优惠海报' and createday >= '2020-01-01'"

    import sparkSession.implicits._
    var dataRdd: RDD[FxPosterClick] = sparkSession.sql(sql).as[FxPosterClick].rdd

    val jsonRdd: RDD[String] = dataRdd.map(item => {
      try {
        val timestamp: String = item.event_time
        val userId: String = item.user_id
        val event: String = "hft_fxPosterClick"
        val lpID_var: String = item.url.split("buildingId=")(1)

        val jsonObj = new JSONObject()
        jsonObj.put("timestamp", timestamp.toLong)
        jsonObj.put("userId", userId)
        jsonObj.put("event", event)

        val attrsObj = new JSONObject()
        attrsObj.put("lpID_var", lpID_var)
        jsonObj.put("attrs", attrsObj)

        jsonObj.toJSONString
      } catch {
        case ex: Exception => {
          ""
        }
      }
    })

    jsonRdd.coalesce(numPartitions, true).saveAsTextFile("hdfs://" + nameNode + ":8020/temp/wangyuhang/hft/fxPosterClick")
  }

  def executeLxPhoneClick(sparkSession: SparkSession): Unit = {
    var sql = "select * from buried_point.ods_ext_hdb_buried_data where length(user_id)>10 and event='WebClick' and title='楼盘详情' and element_content='电话' and createday >= '2020-01-01'"

    import sparkSession.implicits._
    var dataRdd: RDD[LxPhoneClick] = sparkSession.sql(sql).as[LxPhoneClick].rdd

    val jsonRdd: RDD[String] = dataRdd.map(item => {
      try {
        val timestamp: String = item.event_time
        val userId: String = item.user_id
        val event: String = "hft_lxPhoneClick"
        val lxPhone_var: String = item.element_target_url.split("tel:")(1)

        val jsonObj = new JSONObject()
        jsonObj.put("timestamp", timestamp.toLong)
        jsonObj.put("userId", userId)
        jsonObj.put("event", event)

        val attrsObj = new JSONObject()
        attrsObj.put("lxPhone_var", lxPhone_var)
        jsonObj.put("attrs", attrsObj)

        jsonObj.toJSONString
      } catch {
        case ex: Exception => {
          ""
        }
      }
    })

    jsonRdd.coalesce(numPartitions, true).saveAsTextFile("hdfs://" + nameNode + ":8020/temp/wangyuhang/hft/lxPhoneClick")
  }

  def executeLxCustomerClick(sparkSession: SparkSession): Unit = {
    var sql = "select * from buried_point.ods_ext_hdb_buried_data where length(user_id)>10 and event='Pageview' and title='在线咨询' and createday >= '2020-01-01'"

    import sparkSession.implicits._
    var dataRdd: RDD[LxCustomerClick] = sparkSession.sql(sql).as[LxCustomerClick].rdd

    val jsonRdd: RDD[String] = dataRdd.map(item => {
      try {
        val timestamp: String = item.event_time
        val userId: String = item.user_id
        val event: String = "hft_lxCustomerClick"
        val houseID_var: String = item.url.split("houseID=")(1)

        val jsonObj = new JSONObject()
        jsonObj.put("timestamp", timestamp.toLong)
        jsonObj.put("userId", userId)
        jsonObj.put("event", event)

        val attrsObj = new JSONObject()
        attrsObj.put("houseID_var", houseID_var)
        jsonObj.put("attrs", attrsObj)

        jsonObj.toJSONString
      } catch {
        case ex: Exception => {
          ""
        }
      }
    })

    jsonRdd.coalesce(numPartitions, true).saveAsTextFile("hdfs://" + nameNode + ":8020/temp/wangyuhang/hft/lxCustomerClick")
  }

  def executeRegisterPageView(sparkSession: SparkSession): Unit = {
    var sql = "select * from buried_point.ods_ext_hdb_buried_data where length(user_id)>10 and event='Pageview' and title='恒大地产诚邀恒房通会员' and createday >= '2020-01-01'"

    import sparkSession.implicits._
    var dataRdd: RDD[BaseEvent] = sparkSession.sql(sql).as[BaseEvent].rdd

    val jsonRdd: RDD[String] = dataRdd.map(item => {
      try {
        val timestamp: String = item.event_time
        val userId: String = item.user_id
        val event: String = "hft_registerPageView"

        val jsonObj = new JSONObject()
        jsonObj.put("timestamp", timestamp.toLong)
        jsonObj.put("userId", userId)
        jsonObj.put("event", event)

        jsonObj.toJSONString
      } catch {
        case ex: Exception => {
          ""
        }
      }
    })

    jsonRdd.coalesce(numPartitions, true).saveAsTextFile("hdfs://" + nameNode + ":8020/temp/wangyuhang/hft/registerPageView")
  }

  def fileWriter(path: String, data: String): Unit = {
    val out = new FileWriter(path, true)
    out.write(data)
    out.close()
  }
}
