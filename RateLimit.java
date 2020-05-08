package  com.jzy.edu.cloud.common;

import org.apache.log4j.Logger;

import com.jzy.edu.cloud.common.JedisUtil;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

/**
 * @Auther: wk
 * @Date: 2019/5/28 002817:37
 * @Description:
 */
public class RateLimit {


  protected final static org.apache.log4j.Logger logger =Logger.getLogger(RateLimit.class);

  private static final String RATE_LIMIT = "RATELIMIT";

  /**
   * @Title: allow @Description: 进行流量控制,允许访问返回true 不允许访问返回false
   * @param: @param key 放入redis的key，放入前要在key之前添加前缀 前缀配置在eds.properties中的 redis.prefix
   * @param: @param timeOut 超时时间单位秒
   * @param: @param count 超时时间内允许访问的次数
   * @param: @param type 不同类型的数据
   * @param: @return
   * @param: @throws
   *             Exception @return: boolean @throws
   */
  public static boolean allow(String type,String key, int timeOut, int count) {

//        Boolean useFc = Boolean.valueOf(EdsPropertiesUtil.getInstance().getProperty("flowControl.use"));
//        // 若不使用流量控制直接返回true
//        if (!useFc) {
//            return true;
//        }

    boolean result = false;
    Jedis jedis = null;
    StringBuffer keyBuff = new StringBuffer(RATE_LIMIT);
    keyBuff.append("_").append(type).append(":").append(key);
    key = keyBuff.toString();
    try {
      JedisUtil jedisUtil = new JedisUtil();
      jedisUtil.setJedisPool(new JedisPool("47.97.116.199",3306));
      jedis = new Jedis("47.97.116.199",6379);
      jedis.connect();
      Long newTimes = null;
      Long pttl = jedis.pttl(key);

      if (pttl > 0) {
        newTimes = jedis.incr(key);
        if (newTimes > count) {
          logger.info("key:"+key+",超出"+timeOut+"秒内允许访问"+count+"次的限制,这是第"+newTimes+"次访问");
        } else {
          result = true;
        }
      } else if (pttl == -1 || pttl == -2 || pttl == 0) {

        Transaction tx = jedis.multi();
        Response<Long> rsp1 = tx.incr(key);
        tx.expire(key, timeOut);
        tx.exec();
        newTimes = rsp1.get();
        if (newTimes > count) {
          logger.info("key:"+key+","+timeOut+"秒内允许访问"+count+"次,第"+newTimes+"次访问");

        } else {

          result = true;
        }

      }
      if (result) {
        logger.debug("key:"+key+",访问次数+"+count+"");
      }

    } catch (Exception e) {
      logger.error("流量控制发生异常", e);
      e.printStackTrace();
      // 当发生异常时 允许访问
      result = true;
    } finally {
      jedis.close();
    }
    return result;
  }

  public static void main(String[] args) {
    if (RateLimit.allow("RECOMMENDCODE","test", 60, 5)) {
      System.out.println(111);
      //处理业务
    }else{
      System.out.println(222);
      //返回失败
    }
  }
}
