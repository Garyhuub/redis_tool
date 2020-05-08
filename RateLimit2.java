package  com.jzy.edu.cloud.common;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import com.jzy.edu.cloud.common.JedisUtil;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

/**
 *
 * 集群限流操作
 * @Description:
 */
public class RateLimit2 {

@Autowired
private static RedisCache jedisCluster;
  protected final static org.apache.log4j.Logger logger =Logger.getLogger(RateLimit.class);

  private static final String RATE_LIMIT = "RATELIMIT";
//	/**
//	 * redis集群 ip 端口
//	 */
//	private static String hostAndPorts = "192.168.161.195:7000||192.168.161.195:7001||"
//			+ "192.168.161.194:7002||192.168.161.194:7003||" + "192.168.161.193:7004||192.168.161.193:7005";
//	/**
//	 * redis集群
//	 */
//	private static JedisCluster jedisCluster;
//
//	public void setJedisCluster(JedisCluster JedisCluster) {
//		this.jedisCluster = JedisCluster;
//	}
//
//	/**
//	 * 获取jedisCluster集群
//	 *
//	 * @return jedisCluster
//	 */
//	public static JedisCluster getJedisCluster() {
//
//		if (jedisCluster == null) {
//			int timeOut = 10000;
//			Set<HostAndPort> nodes = new HashSet<HostAndPort>();
//			JedisPoolConfig poolConfig = new JedisPoolConfig();
//			poolConfig.setMaxTotal(200);// 最大连接数 使用后需要关闭连接或者释放连接资源否则后续连接无法使用
//			poolConfig.setMaxIdle(50);
//			poolConfig.setMaxWaitMillis(1000 * 100);
//			poolConfig.setTestOnBorrow(false);
////			if(hostAndPorts== null) {
////	        ResourceBundle resourceBundle = ResourceBundle.getBundle("jdbc/jdbc");
////	        hostAndPorts = resourceBundle.getString("redis.hostAndPorts");
////			}
//			String[] hosts = hostAndPorts.split("\\|\\|");
//			for (String hostport : hosts) {
//				String[] ipport = hostport.split(":");
//				String ip = ipport[0];
//				int port = Integer.parseInt(ipport[1]);
//				nodes.add(new HostAndPort(ip, port));
//			}
////			jedisCluster = new JedisCluster(nodes, timeOut, poolConfig);				
//			// 带密码的连接 
//			int connectionTimeout =6000; //连接超时时间 
//			int soTimeout=2000;//返回值的超时时间
//            int maxAttempts=6;//现异常最大重试次数
//            String password="esdflj35sdf1";//密码
//			jedisCluster= new redis.clients.jedis.JedisCluster(nodes, 
//					 connectionTimeout,  soTimeout,
//					 maxAttempts, password,  poolConfig);
//		}
//		return jedisCluster;
//	}

  /**
   * @Title: allow @Description: 进行流量控制,允许访问返回true 不允许访问返回false
   * @param: @param key 放入redis的key，放入前要在key之前添加前缀 前缀配置在eds.properties中的 redis.prefix
   * @param: @param timeOut 超时时间单位秒
   * @param: @param count 超时时间内允许访问的次数
   * @param: @param type 不同类型的数据
   * @param: @return
   * @param: @throws
   * @throws IOException 
   */
  public static boolean allow(String type,String key, int timeOut, int count) throws IOException {
	  
    boolean result = false;
    JedisCluster jedis = null;
    StringBuffer keyBuff = new StringBuffer(RATE_LIMIT);
    keyBuff.append("_").append(type).append(":").append(key);
    key = keyBuff.toString();
    try {
	   jedis = jedisCluster.getJedisCluster();
      Long newTimes = null;
      Long pttl = jedis.ttl(key);

      if (pttl > 0) {
        newTimes = jedis.incr(key);
        if (newTimes > count) {
        	System.out.println("key:"+key+",超出"+timeOut+"秒内允许访问"+count+"次的限制,这是第"+newTimes+"次访问");
        } else {
          result = true;
        }
      } else if (pttl == -1 || pttl == -2 || pttl == 0) {  	  
          Long rsp1 = jedis.incr(key);
          jedis.expire(key, timeOut);
          newTimes =rsp1;
        if (newTimes > count) {
   System.out.println("key:"+key+","+timeOut+"秒内允许访问"+count+"次,第"+newTimes+"次访问");
        } else {
          result = true;
        }

      }
      if (result) {
   System.out.println("key:"+key+",访问次数max为"+count+",当前为:"+newTimes);
      }
    } catch (Exception e) {
    System.out.println("流量控制发生异常");
      e.printStackTrace();
      // 当发生异常时 允许访问
      result = true;
    } finally {
 
    }
    return result;
  }

  public static void main(String[] args) throws IOException {
	  for(int i=0;i<10;i++) {
    if (RateLimit2.allow("RECOMMENDCODE","test", 60, 5)) {
      System.out.println(111);
      //处理业务
    }else{
      System.out.println(222);
      //返回失败
    }
  }
  }
}
