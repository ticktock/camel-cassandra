package org.apache.camel.component.cassandra

import org.scalatest.matchers.ShouldMatchers
import java.lang.String
import collection.immutable.Map
import org.scalatest.{BeforeAndAfterEach, FunSuite, BeforeAndAfterAll}
import org.apache.camel.scala.dsl.builder.RouteBuilder
import collection.jcl.HashMap
import java.util.{Set => JSet}
import org.apache.camel.component.cassandra.CassandraProducer._
import collection.jcl.Conversions._
import org.apache.camel.{Message, Exchange, ExchangePattern, CamelContext}
import org.apache.camel.scala.RichExchange
import org.apache.camel.component.cassandra.ProducerSuite._
import org.apache.camel.component.cassandra.CassandraComponent._
import com.shorrockin.cascal.session._
import com.shorrockin.cascal.utils.Conversions._
import org.apache.camel.builder.ExpressionBuilder
import org.springframework.context.support.ClassPathXmlApplicationContext
import org.apache.camel.impl._
import java.util.Collections
import java.io.ByteArrayInputStream
import com.shorrockin.cascal.testing.{CassandraTestPool, EmbeddedTestCassandra}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class ProducerSuite extends FunSuite with CassandraSuite with ShouldMatchers {
  


  test("sending to an endpoint with keyspace columnfamily and column results in an insert to cassandra") {
    var context = new DefaultCamelContext
    context.addRoutes(new ProducerTestRouteBuilder)
    context.start

    var headers = new HashMap[String, java.lang.Object]
    headers += keyHeader -> "theKey"
    var body = "TEST123"
    var exchange: Exchange = new DefaultExchange(context)
    exchange.getIn.setHeaders(headers)
    exchange.getIn.setBody(body)
    exchange.setPattern(ExchangePattern.InOut)

    var template = new DefaultProducerTemplate(context)
    template.start
    exchange = template.send(inboundSpaceFamilyColumn, exchange)

    val out: Message = exchange.getOut
    out.getHeader(keyspaceHeader, classOf[String]) should be("camel-cassandra")
    out.getHeader(columnFamilyHeader, classOf[String]) should be("stringCols")
    out.getHeader(columnHeader, classOf[String]) should be("testcolumn")
    out.getHeader(keyHeader, classOf[String]) should be("theKey")
    out.getHeader(superColumnHeader) should be(null)
    out.getBody(classOf[String]) should be("TEST123")

    withSession {
      session =>
        session.get("camel-cassandra" \ "stringCols" \ "theKey" \ "testcolumn") match {
          case Some(x) => string(x.value) should be("TEST123")
          case None => fail
        }
    }

  }

  test("sending to an endpoint with keyspace columnfamily and column and Value DataFormat results in an insert to cassandra") {
    var context = new DefaultCamelContext
    val reg = new SimpleRegistry
    context.setRegistry(reg)
    val format = new SerializationDataFormat
    reg.put(dataFormatOption, format)
    context.addRoutes(new ProducerTestRouteBuilder)
    context.start

    var headers = new HashMap[String, java.lang.Object]
    headers += keyHeader -> "theFormatKey"
    var body = Collections.singleton("TEST123")
    var exchange: Exchange = new DefaultExchange(context)
    exchange.getIn.setHeaders(headers)
    exchange.getIn.setBody(body)
    exchange.setPattern(ExchangePattern.InOut)

    var template = new DefaultProducerTemplate(context)
    template.start
    exchange = template.send(inboundSpaceFamilyColumn, exchange)

    val out: Message = exchange.getOut
    out.getHeader(keyspaceHeader, classOf[String]) should be("camel-cassandra")
    out.getHeader(columnFamilyHeader, classOf[String]) should be("stringCols")
    out.getHeader(columnHeader, classOf[String]) should be("testcolumn")
    out.getHeader(keyHeader, classOf[String]) should be("theFormatKey")
    out.getHeader(superColumnHeader) should be(null)

    withSession {
      session =>
        session.get("camel-cassandra" \ "stringCols" \ "theFormatKey" \ "testcolumn") match {
          case Some(x) => {
            val bais = new ByteArrayInputStream(x.value)
            val bodyOut = format.unmarshal(exchange, bais)
            bodyOut.asInstanceOf[JSet[String]].contains("TEST123") should be(true)
            bodyOut.asInstanceOf[JSet[String]].size should be(1)
          }
          case None => fail
        }
    }

  }


  test("sending to an endpoint with keyspace columnfamily supercolumn and column results in an insert to cassandra") {
    var context = new DefaultCamelContext
    context.addRoutes(new ProducerTestRouteBuilder)
    context.start

    var headers = new HashMap[String, java.lang.Object]
    headers += keyHeader -> "theSupercolKey"
    var body = "TEST123"
    var exchange: Exchange = new DefaultExchange(context)
    exchange.getIn.setHeaders(headers)
    exchange.getIn.setBody(body)
    exchange.setPattern(ExchangePattern.InOut)

    var template = new DefaultProducerTemplate(context)
    template.start
    exchange = template.send(inboundSpaceFamilySupercolumnColumn, exchange)

    val out: Message = exchange.getOut
    out.getHeader(keyspaceHeader, classOf[String]) should be("camel-cassandra")
    out.getHeader(columnFamilyHeader, classOf[String]) should be("superStringCols")
    out.getHeader(columnHeader, classOf[String]) should be("testcolumn")
    out.getHeader(keyHeader, classOf[String]) should be("theSupercolKey")
    out.getHeader(superColumnHeader) should be("superduper")
    out.getBody(classOf[String]) should be("TEST123")

    withSession {
      session =>
        session.get("camel-cassandra" \\ "superStringCols" \ "theSupercolKey" \ "superduper" \ "testcolumn") match {
          case Some(x) => string(x.value) should be("TEST123")
          case None => fail
        }
    }

  }

  test("sending to an endpoint with keyspace columnfamily key and supercolumn results in an insert to cassandra") {
    var context = new DefaultCamelContext
    context.addRoutes(new ProducerTestRouteBuilder)
    context.start

    var headers = new HashMap[String, java.lang.Object]
    headers += columnHeader -> "testcolumn"
    var body = "TEST123"
    var exchange: Exchange = new DefaultExchange(context)
    exchange.getIn.setHeaders(headers)
    exchange.getIn.setBody(body)
    exchange.setPattern(ExchangePattern.InOut)

    var template = new DefaultProducerTemplate(context)
    template.start
    exchange = template.send(inboundSpaceFamilyKeySupercolumn, exchange)

    val out: Message = exchange.getOut
    out.getHeader(keyspaceHeader, classOf[String]) should be("camel-cassandra")
    out.getHeader(columnFamilyHeader, classOf[String]) should be("superStringCols")
    out.getHeader(columnHeader, classOf[String]) should be("testcolumn")
    out.getHeader(keyHeader, classOf[String]) should be("testUrlKey")
    out.getHeader(superColumnHeader) should be("superduper")
    out.getBody(classOf[String]) should be("TEST123")

    withSession {
      session =>
        session.get("camel-cassandra" \\ "superStringCols" \ "testUrlKey" \ "superduper" \ "testcolumn") match {
          case Some(x) => string(x.value) should be("TEST123")
          case None => fail
        }
    }

  }



  test("sending to an endpoint with nothing but extractors results in an insert to cassandra") {
    var context = new DefaultCamelContext

    val reg = new SimpleRegistry
    context.setRegistry(reg)
    reg.put("keyspaceEx", ExpressionBuilder.constantExpression("camel-cassandra"))
    reg.put("cfEx", ExpressionBuilder.constantExpression("stringCols"))
    reg.put("colEx", ExpressionBuilder.constantExpression("testcolumn"))
    reg.put("keyEx", ExpressionBuilder.headerExpression(keyHeader))
    reg.put("valEx", ExpressionBuilder.bodyExpression)


    context.addRoutes(new ProducerTestRouteBuilder)
    context.start
    var headers = new HashMap[String, java.lang.Object]
    headers += keyHeader -> "theExtractorKey"
    var body = "TEST123"
    var exchange: Exchange = new DefaultExchange(context)
    exchange.getIn.setHeaders(headers)
    exchange.getIn.setBody(body)
    exchange.setPattern(ExchangePattern.InOut)

    var template = new DefaultProducerTemplate(context)
    template.start
    exchange = template.send(inboundExtractSpaceFamilyColumn, exchange)

    val out: Message = exchange.getOut
    out.getHeader(keyspaceHeader, classOf[String]) should be("camel-cassandra")
    out.getHeader(columnFamilyHeader, classOf[String]) should be("stringCols")
    out.getHeader(columnHeader, classOf[String]) should be("testcolumn")
    out.getHeader(keyHeader, classOf[String]) should be("theExtractorKey")
    out.getHeader(superColumnHeader) should be(null)
    out.getBody(classOf[String]) should be("TEST123")

    withSession {
      session =>
        session.get("camel-cassandra" \ "stringCols" \ "theExtractorKey" \ "testcolumn") match {
          case Some(x) => string(x.value) should be("TEST123")
          case None => fail
        }
    }

  }

  test("sending to an endpoint with nothing but extractors,including supercolumn results in an insert to cassandra") {
    var context = new DefaultCamelContext

    val reg = new SimpleRegistry
    context.setRegistry(reg)
    reg.put("keyspaceEx", ExpressionBuilder.constantExpression("camel-cassandra"))
    reg.put("cfEx", ExpressionBuilder.constantExpression("superStringCols"))
    reg.put("colEx", ExpressionBuilder.constantExpression("testcolumn"))
    reg.put("supEx", ExpressionBuilder.constantExpression("superduper"))
    reg.put("keyEx", ExpressionBuilder.headerExpression(keyHeader))
    reg.put("valEx", ExpressionBuilder.bodyExpression)



    context.addRoutes(new ProducerTestRouteBuilder)
    context.start
    var headers = new HashMap[String, java.lang.Object]
    headers += keyHeader -> "theSuperExtractorKey"
    var body = "TEST123"
    var exchange: Exchange = new DefaultExchange(context)
    exchange.getIn.setHeaders(headers)
    exchange.getIn.setBody(body)
    exchange.setPattern(ExchangePattern.InOut)

    var template = new DefaultProducerTemplate(context)
    template.start
    exchange = template.send(inboundExtractSpaceFamilySuperColumn, exchange)

    val out: Message = exchange.getOut
    out.getHeader(keyspaceHeader, classOf[String]) should be("camel-cassandra")
    out.getHeader(columnFamilyHeader, classOf[String]) should be("superStringCols")
    out.getHeader(columnHeader, classOf[String]) should be("testcolumn")
    out.getHeader(keyHeader, classOf[String]) should be("theSuperExtractorKey")
    out.getHeader(superColumnHeader) should be("superduper")
    out.getBody(classOf[String]) should be("TEST123")

    withSession {
      session =>
        session.get("camel-cassandra" \\ "superStringCols" \ "theSuperExtractorKey" \ "superduper" \ "testcolumn") match {
          case Some(x) => string(x.value) should be("TEST123")
          case None => fail
        }
    }

  }

  test("loading routes and extractors via spring and specifying extractor beans with options and sending to an endpoint results in an insert to cassandra") {
    var spring = new ClassPathXmlApplicationContext("classpath:producer-suite-context-1.xml")
    var context = spring.getBean("camel").asInstanceOf[CamelContext]
    var headers = new HashMap[String, java.lang.Object]
    headers += keyHeader -> "theSpringKey"
    headers += columnFamilyHeader -> "stringCols"
    headers += columnHeader -> "testcolumn"
    var body = "TEST123"
    var exchange: Exchange = new DefaultExchange(context)
    exchange.getIn.setHeaders(headers)
    exchange.getIn.setBody(body)
    exchange.setPattern(ExchangePattern.InOut)

    var template = new DefaultProducerTemplate(context)
    template.start
    exchange = template.send(inboundSpring, exchange)

    val out: Message = exchange.getOut
    out.getHeader(keyspaceHeader, classOf[String]) should be("camel-cassandra")
    out.getHeader(columnFamilyHeader, classOf[String]) should be("stringCols")
    out.getHeader(columnHeader, classOf[String]) should be("testcolumn")
    out.getHeader(keyHeader, classOf[String]) should be("theSpringKey")
    out.getHeader(superColumnHeader) should be(null)
    out.getBody(classOf[String]) should be("TEST123")

    withSession {
      session =>
        session.get("camel-cassandra" \ "stringCols" \ "theSpringKey" \ "testcolumn") match {
          case Some(x) => string(x.value) should be("TEST123")
          case None => fail
        }
    }

  }

  test("loading routes and extractors via spring using default extractor bean names instead of options and sending to an endpoint results in an insert to cassandra") {
    var spring = new ClassPathXmlApplicationContext("classpath:producer-suite-context-2.xml")
    var context = spring.getBean("camel").asInstanceOf[CamelContext]
    var headers = new HashMap[String, java.lang.Object]
    headers += keyHeader -> "theSpringDefaultKey"
    headers += columnFamilyHeader -> "stringCols"
    headers += columnHeader -> "testcolumn"
    var body = "TEST123"
    var exchange: Exchange = new DefaultExchange(context)
    exchange.getIn.setHeaders(headers)
    exchange.getIn.setBody(body)
    exchange.setPattern(ExchangePattern.InOut)

    var template = new DefaultProducerTemplate(context)
    template.start
    exchange = template.send(inboundSpring, exchange)

    val out: Message = exchange.getOut
    out.getHeader(keyspaceHeader, classOf[String]) should be("camel-cassandra")
    out.getHeader(columnFamilyHeader, classOf[String]) should be("stringCols")
    out.getHeader(columnHeader, classOf[String]) should be("testcolumn")
    out.getHeader(keyHeader, classOf[String]) should be("theSpringDefaultKey")
    out.getHeader(superColumnHeader) should be(null)
    out.getBody(classOf[String]) should be("TEST123")

    withSession {
      session =>
        session.get("camel-cassandra" \ "stringCols" \ "theSpringDefaultKey" \ "testcolumn") match {
          case Some(x) => string(x.value) should be("TEST123")
          case None => fail
        }
    }

  }

}

class ProducerTestRouteBuilder extends RouteBuilder {
  from(inboundSpaceFamilyColumn) --> outboundSpaceFamilyColumn
  from(inboundExtractSpaceFamilyColumn) --> outboundExtractSpaceFamilyColumn
  from(inboundSpaceFamilySupercolumnColumn) --> outboundSpaceFamilySupercolumnColumn
  from(inboundExtractSpaceFamilySuperColumn) --> outboundExtractSpaceFamilySuperColumn
  from(inboundSpaceFamilyKeySupercolumn) --> outboundSpaceFamilyKeySupercolumn
}

object ProducerSuite {
  val inboundSpaceFamilyColumn = "direct:inboundSpaceFamilyColumn"
  val outboundSpaceFamilyColumn = "cassandra:/camel-cassandra/stringCols/testcolumn"
  val inboundSpaceFamilySupercolumnColumn = "direct:inboundSpaceFamilySupercolumnColumn"
  val outboundSpaceFamilySupercolumnColumn = "cassandra:/camel-cassandra/superStringCols/!superduper/testcolumn"
  val inboundSpaceFamilyKeySupercolumn = "direct:inboundSpaceFamilyKeySupercolumnColumn"
  val outboundSpaceFamilyKeySupercolumn = "cassandra:/camel-cassandra/superStringCols/~testUrlKey/!superduper"
  val inboundSpring = "direct:spring"
  val outboundSpring = "cassandra:/camel-cassandra?columnFamilyExtractor=cfEx&columnExtractor=colEx"
  val inboundExtractSpaceFamilyColumn = "direct:inboundExtractSpaceFamilyColumn"
  val outboundExtractSpaceFamilyColumn = "cassandra:/?keyspaceExtractor=keyspaceEx&columnFamilyExtractor=cfEx&columnExtractor=colEx&keyExtractor=keyEx&valueExtractor=valEx"
  val inboundExtractSpaceFamilySuperColumn = "direct:inboundExtractSpaceFamilySuperColumn"
  val outboundExtractSpaceFamilySuperColumn = "cassandra:/?keyspaceExtractor=keyspaceEx&columnFamilyExtractor=cfEx&columnExtractor=colEx&keyExtractor=keyEx&valueExtractor=valEx&superColumnExtractor=supEx"

}
