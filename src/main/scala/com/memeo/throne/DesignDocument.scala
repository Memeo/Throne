package com.memeo.throne

class View(val map:Option[String], val reduce:Option[String], val dbcopy:Option[String])

class DesignDocument(val _id:Option[String],
                     val _rev:Option[String],
                     val language:Option[String],
                     val views:Option[Map[String,View]],
                     val shows:Option[Map[String,String]],
                     val validate_doc_update:Option[String])