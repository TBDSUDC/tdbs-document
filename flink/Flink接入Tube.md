# Flink消费案例

注：TBDS的flink的jar包和开源的jar存在部分冲突，建议在自己的项目中的flink jar包的scope都设置为provide

```xml
<dependency>
     <groupId>org.apache.flink</groupId>
     <artifactId>flink-streaming-java_${scala.binary.version}</artifactId>
     <version>${flink.version}</version>
     <scope>provided</scope>
</dependency>
<dependency>
     <groupId>org.apache.flink</groupId>
     <artifactId>flink-streaming-scala_${scala.binary.version}</artifactId>
     <version>${flink.version}</version>
     <scope>provided</scope>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-test-utils_${scala.binary.version}</artifactId>
    <version>${flink.version}</version>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-statebackend-rocksdb_${scala.binary.version}</artifactId>
    <version>${flink.version}</version>
    <scope>provided</scope>
</dependency>
```



### 代码讲解

#### 主类讲解

```scala
package com.tencent.tbds.tdbank;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.tencent.tbds.tdbank.source.TubeSource;

import java.util.HashMap;

  public class AppMain {
  public static void main(String[] args) throws Exception {

      //Tube的相关的配置信息
      HashMap<String, String> map = new HashMap<String, String>(7);
      //tube 的地址
      map.put("tube.ha.hosts", "tbds-10-22xxxx:9100,tbds-10-22xxxx3:9100,tbds-1xxxx:9100");
      /**
       *  消费组的名称，
       *  需要确认：
       *    1. 此消费组需要在tbds界面进行预先申请，审批等等
       *    2. 此消费组的用户需要有此tube的topic的访问权限
       */
      map.put("tube.consumer.group", "CCTV_YSP_FTBFZL_RC_PROD_CCTV_YSP_Sxxxxx_b_omg_vidxxxx");
      //消费的用户名
      map.put("tube.consumer.username", "saxxxxxxx");
      //用户的security_id使用tube模块对应的id
      map.put("tube.consumer.secureId", "Lib68xxxsKngz6Aojvxxxxxxxxxxxx");
      //用户的security_key使用tube模块对应的security_key
      map.put("tube.consumer.secureKey", "ce2OWcS57FUgAPxxxxxxxxxxxxxxxx");
      //Tdbank模块接入的bid的名称
      map.put("tube.consumer.bid", "xxxx");
      //打包的方式
      map.put("tube.consumer.isTdMsg", "false");

      //获取flink的环境信息
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      //加载配置内容
      env.getConfig().setGlobalJobParameters(ParameterTool.fromMap(map));
      //定义数据出处理的窗口方式
      env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
      //添加数据的流入源
      DataStreamSource<String> dataStreamSource = env.addSource(new TubeSource()).setParallelism(1);
      //获取的数据流如何处理
      dataStreamSource.print();
      //任务的名称
      env.execute("TestTube111")
  }

}
```

#### Tube Source的讲解

```java
package com.tencent.tbds.tdbank.source;

import com.google.common.base.Splitter;
import com.tencent.tbds.tdbank.utils.Constants;
import com.tencent.tdbank.msg.TDMsg1;
import com.tencent.tube.Message;
import com.tencent.tube.client.MessageSessionFactory;
import com.tencent.tube.client.TubeClientConfig;
import com.tencent.tube.client.TubeMessageSessionFactory;
import com.tencent.tube.client.consumer.ConsumerConfig;
import com.tencent.tube.client.consumer.pullconsumer.ConsumerResult;
import com.tencent.tube.client.consumer.pullconsumer.PullMessageConsumer;
import com.tencent.tube.cluster.MasterInfo;
import com.tencent.tube.exception.TubeClientException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class TubeSource extends RichParallelSourceFunction<String> {
    private static Logger LOG = Logger.getLogger(TubeSource.class);
    long start = System.currentTimeMillis();
    private transient Map<String, String> globalMap = null;
    private transient String haHosts;
    private transient String consumerGroup;
    private transient String username;
    private transient String secureId;
    private transient String secureKey;
    private transient String topic;
    private transient boolean isTdMsg;
    private transient PullMessageConsumer messagePullConsumer;
    private MessageSessionFactory messageSessionFactory;
    private volatile boolean running = true;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //通过环境变量获取tube的相关的配置
        globalMap = getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap();
        haHosts = globalMap.get("tube.ha.hosts");
        consumerGroup = globalMap.get("tube.consumer.group");
        username = globalMap.get("tube.consumer.username");
        secureId = globalMap.get("tube.consumer.secureId");
        secureKey = globalMap.get("tube.consumer.secureKey");
        topic = globalMap.get("tube.consumer.bid");
        isTdMsg = Boolean.parseBoolean(globalMap.getOrDefault("tube.consumer.isTdMsg", "false"));
        LOG.info("parameter " + haHosts + "," + consumerGroup + "," + username + "," + secureId + "," + secureKey + "," + topic);

        //构造tube的client消费端的配置配置
        TubeClientConfig tubeConfig = new TubeClientConfig();
        tubeConfig.setMasterInfo(new MasterInfo(haHosts));
        ConsumerConfig consumerConfig = new ConsumerConfig(consumerGroup);
        consumerConfig.setConsumeFromMaxOffset(false);
        consumerConfig.setCertificateAndAuthParams(secureId, secureKey, username);

        //根据消费端的配置构建tube的消费session工厂
        this.messageSessionFactory = new TubeMessageSessionFactory(tubeConfig);
        TreeSet<String> fliterSet = null;
        while (true) {
            try {
                //构建获取信息的consumer
                this.messagePullConsumer =
                        messageSessionFactory.createPullConsumer(consumerConfig);
                //consumer订阅topic，和过滤器，当前过滤器为空
                this.messagePullConsumer.subscribe(topic, fliterSet);
                //完成消息的订阅
                messagePullConsumer.completeSubscribe();
                break;
            } catch (Exception e) {
                if (shouldPrint(10000)) {
                    LOG.warn(TubeSource.class.getCanonicalName() + " error:"
                            + e.getMessage(), e);
                }
                if (messagePullConsumer != null) {
                    messagePullConsumer.shutdown();
                }
                Thread.sleep(1000);
            }
        }

        // wait completeSubscribe TODO
        Thread.sleep(30000);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (messagePullConsumer != null) {
            try {
                messagePullConsumer.shutdown();
            } catch (TubeClientException e) {
                LOG.error("TUBE_CLIENT_ERROR : " + e.getMessage());
            }
        }
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        //从消费端获取client的消费id
        String consumerId = messagePullConsumer.getClientId();
        LOG.info("consumerId:" + consumerId);
        while (running) {
            try {
                //根据端口号获取消费的信息
                ConsumerResult result = this.messagePullConsumer.getMessage(topic);
                //判断消费的数据是否成功
                if (result.isSuccess()) {
                    List<Message> messageList = result.getMessageList();
                    //确认信息消费成功
                    ConsumerResult confirmResult = this.messagePullConsumer
                            .confirmConsume(result.getConfirmContext(), true);
                    //判断信息是否发送成功
                    if (confirmResult.isSuccess()) {
                        if (messageList != null && messageList.size() > 0) {
                            processTdMsg(messageList, result.getPartitionKey(), consumerId);
                            sourceContext.collect("123456");
                        }
                    } else {
                        LOG.info("ConfirmConsume failure, errCode is "
                                + confirmResult.getErrCode() + ",Error message is "
                                + confirmResult.getErrInfo());
                    }
                } else {
                    LOG.warn(
                            "Receive messages failure,errorCode is " + result.getErrCode()
                                    + ", Error message is " + result.getErrInfo());
                    if (result.getErrCode() == -1 || result.getErrCode() == 404) {
                        Thread.sleep(1000);
                    }
                }
            } catch (Exception e) {
                LOG.error("[consume message exception trace]:" + e.getMessage(), e);
            }
        }

    }

    @Override
    public void cancel() {
        running = false;
    }

    /**
     * Tube的数据消息模式是做过压缩和加密处理，所以文件是乱码需要进行处理
     * @param messageList
     * @param partitionKey
     * @param consumerId
     * @throws Exception
     */
    private void processTdMsg(List<Message> messageList, String partitionKey,
                              String consumerId) throws Exception {
        for (Message message : messageList) {
            LOG.info("====================process message topic=" + message.getTopic() + ", attribute=" + message.getAttribute() + "===================");
            TDMsg1 tdmsg = TDMsg1.parseFrom(message.getData());
            for (String attr : tdmsg.getAttrs()) {
                LOG.info("====================process attr===================" + attr);
                Iterator<byte[]> it = tdmsg.getIterator(attr);
                if (it != null) {
                    while (it.hasNext()) {
                        // 这里的body即为原始的业务数据
                        byte[] body = it.next();
                        LOG.info("====================process body===================" + new String(body));
                        // attr即为用户发送消息时所附带的属性数据，其内容为类似k1=v1&k2=v2的kv
                        // 结构的字符串，其中包括bid、tid、dt等信息
                        final Splitter splitter = Splitter.on("&");
                        Map<String, String> attrMap = Constants.parseAttr(splitter, attr, "=");
                        String tid = attrMap.get("tid");
                        LOG.info("====================process tid===================" + tid + ",attrMap=" + attrMap);
                        if ("dig_test".equals(tid)) {
                            // 处理接口id为mytid的数据
                        }
                    }
                }
            }
        }
    }

    public boolean shouldPrint(int timeout) {
        if (System.currentTimeMillis() - start > timeout) {
            start = System.currentTimeMillis();
            return true;
        }
        return false;
    }
}
```