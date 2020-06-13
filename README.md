# ntc-jkafka
ntc-jkafka is a module java kafka client.

## Maven
```Xml
<dependency>
    <groupId>com.streetcodevn</groupId>
    <artifactId>ntc-jkafka</artifactId>
    <version>1.0.1</version>
</dependency>
```

## Usage
### Producer
```java
String name = "worker";
String topic = KConfig.getProduceTopic(name, "email");
String msg = "This is email";

// Way 1: Short code
Future<RecordMetadata> ft = KProducerUtil.sendRecordBytes(name, topic, msg);

// Way 2: Detail code
ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(topic, msg.getBytes("UTF-8"));
KProducer<byte[], byte[]> kp = KProducer.getInstance(name);
kp.getProducer().send(record);
```

### Consumer
```java
// Code template example EmailConsumer
public class EmailConsumer {
    private static final Logger log = LoggerFactory.getLogger(EmailConsumer.class);
    
    private int numWorker = 1;
    private KConsumerService service = new KConsumerService();
    private final String name = "worker";

    public EmailConsumer(int numWorker) {
        this.numWorker = numWorker > 0 ? numWorker : 1;
        for (int i=0; i<this.numWorker; i++) {
            EmailWorker ew = new EmailWorker(name);
            service.addKConsumer(ew);
        }
    }
    
    public void start() {
        try {
            service.start();
        } catch (Exception e) {
            log.error("EmailConsumer start " + e.toString(), e);
        }
    }
    
    public void stop() {
        try {
            service.stop();
        } catch (Exception e) {
            log.error("EmailConsumer stop " + e.toString(), e);
        }
    }
    
    public class EmailWorker extends KConsumeLoop<byte[], byte[]> {

        public EmailWorker(String name) {
            super(name);
        }

        @Override
        public void process(ConsumerRecord<byte[], byte[]> record) {
            try {
                System.out.println("====== EmailWorker[" + getId() + "] process ======");
                String topic = record.topic();
                //String key = new String(record.key(), "UTF-8");
                String value = new String(record.value(), "UTF-8");
                System.out.println("topic: " + topic + ", value: " + value);
                //System.out.println(record.toString());
            } catch (Exception e) {
                log.error("EmailWorker process " + e.toString(), e);
            }
        }
    }
}
```


## License
This code is under the [Apache Licence v2](https://www.apache.org/licenses/LICENSE-2.0).  
