## HDFS数据访问	

### 引入依赖的jar包

此版本hadoop的访问最好使用tbds版本对应的jar包，这样会避免因为包的版本不一致，或者包的依赖冲突等问题。

```xml
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-common</artifactId>
    <version>2.7.2-TBDS-4.0.3.3</version>
</dependency>
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-hdfs</artifactId>
    <version>2.7.2-TBDS-4.0.3.3</version>
</dependency>
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-client</artifactId>
    <version>2.7.2-TBDS-4.0.3.3</version>
</dependency>
```



### 导入hdfs-site.xml core-site.xml

hdfs的访问需要依赖相关的配置，需要加入配置文件。

```java
private static Configuration conf = new Configuration();

conf.addResource(new FileInputStream("/etc/hadoop/conf/core-site.xml"));
conf.addResource(new FileInputStream("/etc/hadoop/conf/hdfs-site.xml"));
conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
```



### 代码详解

```java
public class WebhdfsDemo {
	//日志文件
    private static Logger logger = LoggerFactory.getLogger(WebhdfsDemo.class);
    /**hadoop 相干的配置信息***/
    private static Configuration conf = new Configuration();

    // TBDS的认证信息加入
    static {
        conf.set("hadoop_security_authentication_tbds_username", "hdfs");
        conf.set("hadoop_security_authentication_tbds_secureid", "YILf620OFPXSDV15HPhonYg9ZzinB91llkNE");
        conf.set("hadoop_security_authentication_tbds_securekey", "1JsMBBMGXIQCtp69sRRNagNfLkyLoRLN");
    }

    /**
     *  HDFS配置信息注入
     */
    private static void initConfiguration(){
        try {
            conf.addResource(new FileInputStream("/etc/hadoop/conf/core-site.xml"));
            conf.addResource(new FileInputStream("/etc/hadoop/conf/hdfs-site.xml"));
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        }catch (Exception e){
            logger.error("Initial component configuration fail, please check it ",e);
        }
    }

    /**运行代码**/
    public static void main(String[] args) {
        try {
            //初始化配置
            initConfiguration();
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromSubject(null);
			//获取文件系统
            FileSystem fs = FileSystem.get(conf);
            
			//调用文件系统的api的方法获取文件列表信息
            FileStatus[] fileList = fs.listStatus(new Path("/"));
            //Testing for search root dir file information
            for (FileStatus fileStatus : fileList) {
                System.out.println(fileStatus.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
```



### 项目打包

```
cd  项目根目录

mvn clean package
```

[https://github.com/TBDSUDC/TBDSDemo/tree/master/%E7%94%A8%E6%88%B7%E6%89%8B%E5%86%8C%E9%9C%80%E8%A6%81%E7%94%A8%E5%88%B0%E7%9A%84%E7%A4%BA%E4%BE%8B%E7%A8%8B%E5%BA%8F/linux%E7%8E%AF%E5%A2%83%E4%B8%8B%E6%93%8D%E4%BD%9CHDFS%E6%A1%88%E4%BE%8B](https://github.com/TBDSUDC/TBDSDemo/tree/master/用户手册需要用到的示例程序/linux环境下操作HDFS案例)



