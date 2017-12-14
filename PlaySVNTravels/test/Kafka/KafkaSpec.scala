package Kafka

import org.scalatest.{FlatSpec, Matchers}

class KafkaSpec extends FlatSpec with Matchers{


  it should """match localhost""" in{
    val propSet = AmadeusProducer.props.getProperty("bootstrap.servers")
    val port = propSet.split(":")(0)
    port should matchPattern{
      case "localhost" =>
    }
  }

  it should """match 9092""" in{
    val propSet = AmadeusProducer.props.getProperty("bootstrap.servers")
    val port = propSet.split(":")(1)
    port should matchPattern{
      case "9092" =>
    }
  }

  it should """match org.apache.kafka.common.serialization.StringSerializer for key""" in{
    val prop = AmadeusProducer.props.getProperty("key.serializer")
    prop should matchPattern{
      case "org.apache.kafka.common.serialization.StringSerializer" =>
    }
  }

  it should """match org.apache.kafka.common.serialization.StringSerializer for value""" in{
    val prop = AmadeusProducer.props.getProperty("value.serializer")
    prop should matchPattern{
      case "org.apache.kafka.common.serialization.StringSerializer" =>
    }
  }

  it should """match Amadeus""" in{
    val topic = AmadeusProducer.TOPIC
    topic should matchPattern{
      case "Amadeus" =>
    }
  }

  /**************************************************************************************************************/

  it should """match localhost for spark producer""" in{
    val propSet = SparkStreamProducer.props.getProperty("bootstrap.servers")
    val port = propSet.split(":")(0)
    port should matchPattern{
      case "localhost" =>
    }
  }

  it should """match 9092 spark producer""" in{
    val propSet = SparkStreamProducer.props.getProperty("bootstrap.servers")
    val port = propSet.split(":")(1)
    port should matchPattern{
      case "9092" =>
    }
  }

  it should """match org.apache.kafka.common.serialization.StringSerializer for key in spark producer""" in{
    val prop = SparkStreamProducer.props.getProperty("key.serializer")
    prop should matchPattern{
      case "org.apache.kafka.common.serialization.StringSerializer" =>
    }
  }

  it should """match org.apache.kafka.common.serialization.StringSerializer for value in spark producer""" in{
    val prop = AmadeusProducer.props.getProperty("value.serializer")
    prop should matchPattern{
      case "org.apache.kafka.common.serialization.StringSerializer" =>
    }
  }

  it should """match sparkData """ in{
    val topic = SparkStreamProducer.TOPIC
    topic should matchPattern{
      case "sparkData" =>
    }
  }

}
