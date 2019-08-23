## Hippo的数据接入与消费

### 引入jar包

```xml
<dependency>
    <groupId>com.tencent.hippo</groupId>
    <artifactId>hippo-client</artifactId>
    <version>1.9-tbds</version>
</dependency>
```



### 登录TBDS的portal界面创建hippo的topic

![1565939130814](C:\Users\danielmou\AppData\Roaming\Typora\typora-user-images\1565939130814.png)



![1565939191328](C:\Users\danielmou\AppData\Roaming\Typora\typora-user-images\1565939191328.png)

### 然后计入消费申请，需要创建Hippo的消费组

![1565939241772](C:\Users\danielmou\AppData\Roaming\Typora\typora-user-images\1565939241772.png)

![1565939255606](C:\Users\danielmou\AppData\Roaming\Typora\typora-user-images\1565939255606.png)



### Hippo 生产者代码示例

```java
package demo;

import com.tencent.hippo.Message;
import com.tencent.hippo.client.producer.ProducerConfig;
import com.tencent.hippo.client.producer.SendResult;
import com.tencent.hippo.client.producer.SimpleMessageProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  <h1>This is used for access hippo</h1>
 */
public class HippoProduceDemo {

    /**logger**/
    private static Logger logger = LoggerFactory.getLogger(HippoProduceDemo.class);

    //Hipper Controller address
    private static final String CONTROLLER_IP_LIST="10.xx.128.xx:8066";
    //Hipper Borker group
    private static final String BROKER_GROUP="hippoBrokerGroup";
    //Hipper Produce Configure
    private static ProducerConfig  producerConfig;

    /***Producer****/

    //Hipper topice accessId
    private static final String ACCESS_ID="xxxx";
    //Hipper topice accessKey(Note: Each topic's key and id are different)
    private static final String ACCESS_KEY="xxxxx";
    //Hipper access user
    private static final String USERNAME="admin";

    //topic
    private static final String[] topics = {"cctv_demo"};

    //对生产者进行配置话
    static {
        /***producer**/
        producerConfig= new ProducerConfig(CONTROLLER_IP_LIST, BROKER_GROUP);
        producerConfig.setSecretId(ACCESS_ID);
        producerConfig.setSecretKey(ACCESS_KEY);
        producerConfig.setUserName(USERNAME);
    }

    /**Testing for sending data**/
    public static void main(String[] args) throws Exception{
        //Build testing message
        SimpleMessageProducer smp = new SimpleMessageProducer(producerConfig);
        //Register topic
        for(String topic : topics){
            smp.publish(topic);
        }
        // build information.
        SendResult sendResult = null;
        for(int i=0;i<=100;i++){
            String content = "td:td1,content:11111";
            Message msg = new Message("cctv_demo",content.getBytes());
            sendResult = smp.sendMessage(msg);
            if(sendResult.isSuccess()){
                System.out.println("scuccessfull id is "+i);
                logger.info("Send successfull【"+i+"】");
            }else{
                logger.error("Send fail【"+i+"】"+sendResult.getCode());
            }
        }
    }
}
```



### Hippo消费者示例

```java
package demo;


import com.tencent.hippo.Message;
import com.tencent.hippo.client.MessageResult;
import com.tencent.hippo.client.consumer.ConsumerConfig;
import com.tencent.hippo.client.consumer.PullMessageConsumer;
import com.tencent.hippo.common.HippoConstants;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *  Hippo is message broker component, This is used for testing consumer
 */
@Slf4j
public class HippoConsumerDemo {

    //Hipper Controller address
    private static final String CONTROLLER_IP_LIST="xxxx.xxx.xxx.xxx:8066";
    //Hipper Borker group
    private static final String GROUP_NAME="tx01_group";
    //Hipper Consumer Configure
    private static ConsumerConfig consumerConfig;
    /***Producer****/
    //Hipper topice accessKey(Note: Each topic's key and id are different)
    private static final String ACCESS_KEY="xxxxxxx";
    //Hipper topice accessId
    private static final String ACCESS_ID="xxxxxxxx";
    //Hipper access user
    private static final String USERNAME="admin";
    //Topic
    private static final String TOPIC="tx01";
    //Batch size
    private static final int BATCH_SIZE=10;
    //timeout
    private static final int TIME_OUT=10000;

    //Initial consumer configuration
    static {
        consumerConfig = new ConsumerConfig(CONTROLLER_IP_LIST,GROUP_NAME);
        consumerConfig.setSecretId(ACCESS_ID);
        consumerConfig.setSecretKey(ACCESS_KEY);
        consumerConfig.setUserName(USERNAME);
        consumerConfig.setConfirmTimeout(10000, TimeUnit.SECONDS);
    }

    /**
     * main for testing
     * @param args
     */
    public static void main(String[] args) throws Exception{
        //获取相关信息的comsumer
        PullMessageConsumer consumer = new PullMessageConsumer(consumerConfig);
        //每次接受信息的大小
        MessageResult result = consumer.receiveMessages(TOPIC,BATCH_SIZE,TIME_OUT);

        if(result.isSuccess()){
            List<Message> msgs = (List<Message>) result.getValue();
            for(Message msg:msgs){
                //header Information
                System.out.println(msg.getHeaders());
               // String msgBody = new String(msg.getData(), Charsets.UTF_8);
                String msgBody = new String(msg.getData());
            }

            boolean confirmed = result.getContext().confirm();

           if(!confirmed){
               log.info("消费信息confirmed的值为："+confirmed);
           }else if(result.getCode()== HippoConstants.NO_MORE_MESSAGE){
               log.info("没有更多的信息消费,进程休息10瞄准");
               Thread.sleep(10000);
           }else {
               System.out.println("Hippo "+result.getCode());
               log.info("Hippo的confirm的状态code值为："+confirmed+"返回的值为："+result.getCode());
           }

           //消费者关闭
            log.info("consumer 关闭");
           consumer.shutdown();
        }
    }
}
```



### 打包成一个可执行jar

```
	mvn clean package
```

### 项目DEMO下载

```
[hippo项目下载地址]（https://github.com/TBDSUDC/TBDSDemo/tree/master/%E7%94%A8%E6%88%B7%E6%89%8B%E5%86%8C%E9%9C%80%E8%A6%81%E7%94%A8%E5%88%B0%E7%9A%84%E7%A4%BA%E4%BE%8B%E7%A8%8B%E5%BA%8F/hippo%E5%BC%80%E5%8F%91%E4%BD%BF%E7%94%A8%E6%A1%88%E4%BE%8B）
```

