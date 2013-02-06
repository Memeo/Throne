package com.memeo.throne

import java.io.IOException
import java.io.InputStreamReader
import java.io.Reader
import java.net.{URL, InetSocketAddress}
import java.net.URLEncoder.encode
import java.nio.charset.Charset
import java.util.concurrent.TimeUnit._
import java.util.logging.Level
import java.util.logging.Logger

import scala.collection.mutable.MapBuilder

import org.apache.commons.codec.binary.Base64
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBufferInputStream
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http.DefaultHttpRequest
import org.jboss.netty.handler.codec.http.HttpMethod._
import org.jboss.netty.handler.codec.http.HttpRequest
import org.jboss.netty.handler.codec.http.HttpResponse
import org.jboss.netty.handler.codec.http.HttpResponseStatus._
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import org.jboss.netty.handler.codec.http.HttpVersion._

import com.twitter.finagle.Service
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.Http
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.Duration
import com.twitter.util.Future

import net.liftweb.json._
import net.liftweb.json.JsonDSL._

class Config(val connectionLimit: Int = 100, val timeout: Duration = Duration(1, MINUTES), val stats:Option[StatsReceiver] = None)

class Auth(user: String, password: String)
{
  def authString() : String = user + ":" + password
  def authBytes() : Array[Byte] = authString().getBytes()
}

class CouchDBException(val error:Option[String] = None, val reason:Option[String] = None, val code:Option[HttpResponseStatus] = None)
      extends IOException(String.format("error:%s reason:%s; code:%s", error.orElse(Some("")).get, reason.orElse(Some("")).get, code.orNull))
{
    def withCode(code:HttpResponseStatus) : CouchDBException = new CouchDBException(error, reason, Some(code))
}

object Util
{
    def reader(buf:ChannelBuffer, enc:String = "UTF-8") : Reader =
        new InputStreamReader(new ChannelBufferInputStream(buf), enc)
}

object Cloudant
{
    class R(val r:Int)
    class W(val w:Int)
    class Q(val q:Int)
    class N(val n:Int)
    object R { def apply(r:Int) = new R(r) }
    object W { def apply(w:Int) = new W(w) }
    object Q { def apply(q:Int) = new Q(q) }
    object N { def apply(n:Int) = new N(n) }
}

class Server(val host: String, val port: Int, auth: Option[Auth] = None, config: Config = new Config())
{
    import Cloudant.{Q,N}
    import Util._
    val logger = Logger.getLogger(classOf[Server].getName)
  
    val client: Service[HttpRequest, HttpResponse] = { val cb = ClientBuilder()
            .codec(Http())
            .hosts(new InetSocketAddress(host, port))
            .hostConnectionLimit(config.connectionLimit)
            .timeout(config.timeout)
            .connectTimeout(config.timeout)
            .tcpConnectTimeout(config.timeout)
            config.stats match {
                case sr:Some[StatsReceiver] => cb.reportTo(sr.get).build() 
                case _ => cb.build()
            }
    }

    val url:URL = new URL("http://" + host + ":" + port)
    
    private[throne] def parseResult[T <: JValue](r:HttpResponse, allowed:Set[HttpResponseStatus] = Set(OK)) : T = {
       if (allowed.contains(r.getStatus()))
       {
           JsonParser.parse(reader(r.getContent())) match {
             case o:T => o
             case x => throw new RuntimeException("expected specific type but got " + x)
           }
       }
       else
       {
           try
           {
               JsonParser.parse(reader(r.getContent())) match {
                   case o:JObject => throw new CouchDBException(o.values.get("error").asInstanceOf[Option[String]],
                                                                o.values.get("reason").asInstanceOf[Option[String]],
                                                                Some(r.getStatus))
               }
           }
           catch
           {
             case e:Exception => throw new CouchDBException(None, None, Some(r.getStatus))
           }
       }
    }

    def info : Future[JObject] = {
        get("/").map {
            r => parseResult[JObject](r, Set(OK))
        }
    }
    
    def allDatabases : Future[JArray] = {
        get("/_all_dbs").map {
            r => parseResult[JArray](r, Set(OK))
        }
    }
    
    def createDatabase(name: String, params: Map[String,String] = Map())(implicit n:Option[N] = None, q:Option[Q] = None) : Future[Database] = {
        val p = new MapBuilder[String, String, Map[String, String]](params)
        if (n.isDefined) { 
            p += ("n" -> n.get.n.toString)
        }
        if (q.isDefined) {
            p += ("q" -> q.get.q.toString)
        }
        put("/" + name, None, headers = Map("Content-type" -> "application/json"), params = p.result).map
        {
            r =>
              parseResult[JObject](r, Set(CREATED, ACCEPTED, PRECONDITION_FAILED))
              new Database(this, name)
        }
    }
    
    def getDatabase(name: String) : Future[Database] = {
        get("/" + name).map
        {
            r => 
              val info = parseResult[JObject](r, Set(OK))
              new Database(this, name)
        }
    }

    def deleteDatabase(name: String) : Future[JObject] = {
        delete("/" + name).map
        {
            r => parseResult[JObject](r, Set(OK, NOT_FOUND))
        }
    }
    
    def paramstr(params: Map[String, String]) : String = {
        params match {
          case m if m.isEmpty => ""
          case m if !m.isEmpty => {
              val x = m.map((e) => encode(e._1, "UTF-8") + "=" + encode(e._2, "UTF-8"))
              "?" + x.tail.foldLeft(x.head)((a, e) => a + "&" + e)
          }
        }
    }
    
    def delete(uri: String, headers: Map[String, Any] = Map(), params: Map[String, String] = Map()) : Future[HttpResponse] = {
        val req: HttpRequest = new DefaultHttpRequest(HTTP_1_1, DELETE, uri + paramstr(params))
        headers foreach { case(key, value) => req.addHeader(key, value) }
        request(req)
    }
    
    def head(uri: String, headers: Map[String, Any] = Map(), params: Map[String, String] = Map()) : Future[HttpResponse] = {
        val req: HttpRequest = new DefaultHttpRequest(HTTP_1_1, HEAD, uri + paramstr(params))
        headers foreach { case(key, value) => req.addHeader(key, value) }
        request(req)
    }

    def get(uri: String, headers: Map[String, Any] = Map(), params: Map[String, String] = Map()) : Future[HttpResponse] = {
        val req: HttpRequest = new DefaultHttpRequest(HTTP_1_1, GET, uri + paramstr(params))
        headers foreach { case(key, value) => req.addHeader(key, value) }
        request(req)
    }

    def put(uri: String, body: Option[String] = None, headers: Map[String, Any] = Map(), params: Map[String, String] = Map()) : Future[HttpResponse] = {
        val req: HttpRequest = new DefaultHttpRequest(HTTP_1_1, PUT, uri + paramstr(params))
        body match {
          case s : Some[String] => {
              val buf = ChannelBuffers.wrappedBuffer(s.get.getBytes)
              req.setContent(buf)
              req.addHeader("Content-length", buf.readableBytes())
          }
          case None => req.setContent(ChannelBuffers.buffer(0))
        }
        headers foreach { case(key, value) => req.addHeader(key, value) }
        request(req)
    }
    
    def post(uri: String, body: Option[String] = None, headers: Map[String, Any] = Map(), params: Map[String, String] = Map()) : Future[HttpResponse] = {
        val req: HttpRequest = new DefaultHttpRequest(HTTP_1_1, POST, uri + paramstr(params))
        body match {
          case s : Some[String] => {
              val buf = ChannelBuffers.wrappedBuffer(s.get.getBytes)
              req.setContent(buf)
              req.addHeader("Content-length", buf.readableBytes())
          }
          case None => req.setHeader("Content-length", 0)
        }
        headers foreach { case(key, value) => req.addHeader(key, value) }
        request(req)
    }
    
    def request(req: HttpRequest) : Future[HttpResponse] = {
        auth match {
          case a:Some[Auth] => req.addHeader("Authorization", "Basic " + Base64.encodeBase64String(a.get.authBytes))
          case _ => Unit
        }
        req.addHeader("Accept", "*/*")
        req.addHeader("Host", host + ":" + port)
        logger.info(req.toString)
        client(req).map(f => {
          logger.log(Level.INFO, "got response {0}", f.toString)
          logger.log(Level.INFO, "body: {0}", f.getContent.toString(Charset.forName("UTF-8")))
          f
        })
    }
}