package com.memeo.throne

import com.twitter.util.Future
import com.twitter.util.Promise
import org.jboss.netty.handler.codec.http.HttpResponseStatus._
import java.net.URLEncoder.encode
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http.HttpResponse
import net.liftweb.json._
import scala.collection.mutable.MapBuilder
import java.util.logging.Logger
import java.net.URL

class Database(val server: Server, val dbname: String)
{
    import Util._
    import Serialization._
    import Cloudant.{R,W}
    implicit val formats = Serialization.formats(NoTypeHints)
    val url = new URL(server.url, "/" + dbname)
    
    val logger = Logger.getLogger(classOf[Database].getName)
    
    def info : Future[JObject] = {
        server.get("/" + dbname).map {
          r => server.parseResult[JObject](r, Set(OK))
        }
    }

    def get(name:String, rev:Option[String] = None)(implicit r:Option[R]=None) : Future[JObject] = {
        val p = new MapBuilder[String,String,Map[String,String]](Map())
        if (rev.isDefined) p += ("rev" -> rev.get)
        if (r.isDefined) p += ("r" -> r.get.r.toString)
        server.get("/" + dbname + "/" + encode(name, "UTF-8"), params = p.result).map
        {
            r => server.parseResult[JObject](r, Set(OK))
        }
    }
    
    def getDesign(name:String, rev:Option[String] = None)(implicit r:Option[R]=None) : Future[JObject] = {
        val p = new MapBuilder[String,String,Map[String,String]](Map())
        if (rev.isDefined) p += ("rev" -> rev.get)
        if (r.isDefined) p += ("r" -> r.get.r.toString)
        server.get("/" + dbname + "/_design/" + encode(name, "UTF-8"), params=p.result).map
        {
            r => server.parseResult[JObject](r, Set(OK))
        }
    }
    
    def put(name:Some[String], obj:JObject)(implicit w:Option[W] = None) : Future[JObject] = {
        val params:Map[String,String] = w match {
          case ww:Some[W] => Map("w" -> ww.get.w.toString)
          case _ => Map()
        }
        name match {
          case n:Some[String] =>
              server.put("/" + dbname + "/" + encode(n.get, "UTF-8"), body=Some(compact(render(obj))), params = params, headers = Map("Content-type" -> "application/json")).map
              {
                  r => server.parseResult[JObject](r, Set(OK, CREATED, ACCEPTED))
              }
          case _ =>
              obj.values.get("_id") match {
                case n:Some[String] =>
                  server.put("/" + dbname + "/" + encode(n.get, "UTF-8"), body=Some(compact(render(obj))), params = params, headers = Map("Content-type" -> "application/json")).map
                  {
                      r => server.parseResult[JObject](r, Set(OK, CREATED, ACCEPTED))
                  }
                case _ =>
                  server.post("/" + dbname, body=Some(compact(render(obj))), headers=Map("Content-type" -> "application/json")).map
                  {
                      r => server.parseResult[JObject](r, Set(OK, CREATED, ACCEPTED))
                  }
              }
        }
    }
    
    def putDesign(name:String, doc:JObject)(implicit w:Option[W] = None) : Future[JObject] = {
        val params:Map[String,String] = w match {
          case ww:Some[W] => Map("w" -> ww.get.w.toString)
          case _ => Map()
        }
        server.put("/" + dbname + "/_design/" + encode(name, "UTF-8"), body=Some(compact(render(doc))), params=params).map
        {
            r => server.parseResult[JObject](r, Set(OK, CREATED, ACCEPTED))
        }
    }
    
    def delete(name:String, rev:String) : Future[JObject] = {
        server.delete("/" + dbname + "/" + encode(name, "UTF-8"), params = Map("rev" -> rev)).map
        {
            r => server.parseResult[JObject](r, Set(OK, NOT_FOUND))
        }
    }
    
    def allDocs(options:Map[String,String] = Map()) : Future[JObject] = {
        server.get("/" + dbname + "/_all_docs", params = options).map
        {
            r => server.parseResult[JObject](r)
        }
    }
    
    def view(design:String, view:String, query:Option[JObject] = None, options:Map[String,String] = Map()) : Future[JObject] = {
        query match {
          case q:Some[JObject] => server.post("/" + dbname + "/_design/" + encode(design, "UTF-8") + "/_view/" + encode(view, "UTF-8"), body=Some(compact(render(q.get)))).map
          {
              r => server.parseResult[JObject](r)
          }
          case _ => server.get("/" + dbname + "/_design/" + encode(design, "UTF-8") + "/_view/" + encode(view, "UTF-8"), params = options).map
          {
              r => server.parseResult[JObject](r)
          }
        }
    }
    
    def changes(longpoll:Boolean = false, options:Map[String,String] = Map()) : Future[JObject] = {
        val params = if (longpoll) options ++ Map("feed" -> "longpoll") else options
        server.get("/" + dbname + "/_changes", params = params).map
        {
            r => server.parseResult[JObject](r)
        }
    }
}