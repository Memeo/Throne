package com.memeo.throne

import net.liftweb.json._

class Row(val id:String, val key:String, val value:Any, doc:Option[JObject])
class ViewResults(val total_rows:Long, val offset:Long, rows:List[Row])

