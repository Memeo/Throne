package com.memeo.throne

import net.liftweb.json.Serialization
import net.liftweb.json.NoTypeHints
import scala.runtime.IntRef
import net.liftweb.json.JObject

class ChangeSpec(val rev:String)
abstract class Change
case class CouchChange(val seq:Long, val id:String, val changes:List[ChangeSpec], val deleted:Option[Boolean], val doc:Option[JObject]) extends Change
//case class BigCouchChange(val seq:List[String], val id:String, val changes:List[ChangeSpec], val deleted:Option[Boolean], val doc:Option[JObject]) extends Change(seq)
case class CloudantChange(val seq:String, val id:String, val changes:List[ChangeSpec], val deleted:Option[Boolean], val doc:Option[JObject]) extends Change
abstract class Changes
case class CouchChanges(val results:List[Change], val last_seq:Long) extends Changes
case class CloudantChanges(val results:List[Change], val last_seq:String) extends Changes