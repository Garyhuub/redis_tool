package com.jzy.edu.cloud.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.util.JedisClusterCRC16;

/**
 * redis 缓存配置
 * 
 */

@Service
public class RedisCache implements Serializable {
	/**
	 * 日志记录
	 */

	private static final Logger LOG  =  LoggerFactory.getLogger(RedisCache.class);
	/**
	 * redis集群
	 */
	private static JedisCluster jedisCluster;

	public void setJedisCluster(JedisCluster JedisCluster) {
		this.jedisCluster = JedisCluster;
	}

	/**
	 * redis集群 ip 端口
	 */
	// 开发环境
	private static String dev_hostAndPorts = "192.168.161.195:7000||192.168.161.195:7001||"
			+ "192.168.161.194:7002||192.168.161.194:7003||" + "192.168.161.193:7004||192.168.161.193:7005";
	// 正式环境
//	private static String dev_hostAndPorts = "172.16.116.16:7000||172.16.116.16:7001||"
//			+ "172.16.116.12:7002||172.16.116.12:7003||" + "172.16.116.21:7004||172.16.116.21:7005";
	/**
	 * 获取jedisCluster集群
	 *
	 * @return jedisCluster
	 */
	public static JedisCluster getJedisCluster() {

		if (jedisCluster == null) {
			int timeOut = 10000;
			Set<HostAndPort> nodes = new HashSet<HostAndPort>();
			JedisPoolConfig poolConfig = new JedisPoolConfig();
			poolConfig.setMaxTotal(200);// 最大连接数 使用后需要关闭连接或者释放连接资源否则后续连接无法使用
			poolConfig.setMaxIdle(50);
			poolConfig.setMaxWaitMillis(1000 * 100);
			poolConfig.setTestOnBorrow(false);
//			if(hostAndPorts== null) {
//	        ResourceBundle resourceBundle = ResourceBundle.getBundle("jdbc/jdbc");
//	        hostAndPorts = resourceBundle.getString("redis.hostAndPorts");
//			}
			String osName = System.getProperty("os.name");
			String[]	 hosts = dev_hostAndPorts.split("\\|\\|");
			for (String hostport : hosts) {
				String[] ipport = hostport.split(":");
				String ip = ipport[0];
				int port = Integer.parseInt(ipport[1]);
				nodes.add(new HostAndPort(ip, port));
			}
		//	jedisCluster = new JedisCluster(nodes, timeOut, poolConfig);				
			// 带密码的连接 
			int connectionTimeout =6000; //连接超时时间 
			int soTimeout=2000;//返回值的超时时间
            int maxAttempts=6;//现异常最大重试次数
            String password="esdflj35sdf1";//密码
			jedisCluster= new redis.clients.jedis.JedisCluster(nodes, 
					 connectionTimeout,  soTimeout,
					 maxAttempts, password,  poolConfig);
		}
		return jedisCluster;
	}

	/**
	 * 关闭连接
	 *
	 * @param JedisCluster
	 */
	public void disconnect(JedisCluster JedisCluster) {
		try {
			JedisCluster.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 将jedis 返还连接池
	 *
	 * @param jedis
	 */
	public static void returnJedisClusterResource(JedisCluster JedisCluster) {
		if (null != JedisCluster) {
			try {
				jedisCluster.close();
			} catch (Exception e) {
				LOG.info("can't return jedis to jedisPool");
				e.printStackTrace();
			}
		}
	}

	/**
	 * 向redis中设置字符串内容
	 * 
	 * @param String
	 *            key
	 * @param String
	 *            value
	 * @return
	 * @throws Exception
	 */
	public static void set(String key, String value) {
		JedisCluster jedisCluster = getJedisCluster();
		// SET k1 v1 覆写旧值，无视类型，如有TTL 将被清除。任何key都将回归普通key，v形式
		jedisCluster.set(key.getBytes(), serialize(value));
	}

	/**
	 * 向redis中设置对象
	 * 
	 * @param String
	 *            key
	 * @param Object
	 *            value
	 * @return
	 * @throws Exception
	 */
	public static void set(String key, Object value) {
		JedisCluster jedisCluster = getJedisCluster();
		// SET k1 v1 覆写旧值，无视类型，如有TTL 将被清除。任何key都将回归普通key，v形式
		jedisCluster.set(key.getBytes(), serialize(value));
	}
	
	/**
	 * 自增
	 * 
	 * @param String
	 *            key
	 * @return
	 * @throws Exception
	 */
	public static Long incr(String key) {
		JedisCluster jedisCluster = getJedisCluster();
		return jedisCluster.incr(key);
	}
	
	/**
	 * 自增过期
	 * 
	 * @param String
	 *            key
	 * @param int
	 *            s 过期时间  秒
	 * @return
	 * @throws Exception
	 */
	public static Long increxpire(String key, int s) {
		JedisCluster jedisCluster = getJedisCluster();
		Long incr = jedisCluster.incr(key);
		expire(key,s);
		return incr;
	}

	/**
	 * 向缓存中设置对象和过期时间
	 * 
	 * @param key
	 * @param value
	 *            值
	 * @param type
	 *            NX是不存在时才set， XX是存在时才set
	 * @param 时间单位
	 *            EX秒 PX毫秒
	 * @param 时长
	 * @return
	 */
	public static boolean setsession(String key, String type, Object u, int time) {
		try {
			JedisCluster jedisCluster = getJedisCluster();
	//		System.out.println("key:"+key+"type:"+type+"u:"+u+"time:"+time);
			// key value NX不存在设置XX存在设置 时间单位EX秒 PX毫秒 时间
			jedisCluster.set(key.getBytes(), serialize(u), type.getBytes(), "EX".getBytes(), time);
			// SET k1 v1 NX PX 30000 key不存在时生效，设置过期时间为30000毫秒
			// SET k1 v1 NX EX 30 key不存在时生效，设置过期时间为30秒
			// SET k1 v1 XX EX 300 key * 存在 *时生效，设置过期时间为300秒
			// SET k1 v1 覆写旧值，无视类型，如有TTL 将被清除。任何key都将回归普通key，v形式
			return true;
		} catch (Exception e) {
			// TODO: handle exception
			return false;
		}
	}

	/**
	 * 根据key 获取内容
	 * 
	 * @param key
	 * @return
	 */
	public static Object get(String key) {
		Object value = null;
		JedisCluster jedisCluster = getJedisCluster();
		value = unserialize(jedisCluster.get(key.getBytes()));
		return value;
	}

	/**
	 * 根据key 获取对象
	 * 
	 * @param key
	 * @return
	 */
	public static <T> T get(String key, Class<T> clazz) {

		JedisCluster jedisCluster = getJedisCluster();
		String value = unserialize(jedisCluster.get(key.getBytes())).toString();
		return JSON.parseObject(value, clazz);
	}

	/**
	 * 删除缓存中得对象，根据key
	 * 
	 * @param key
	 * @return
	 */
	public static boolean del(String key) {
		try {
			JedisCluster jedisCluster = getJedisCluster();
			jedisCluster.del(key.getBytes());
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {
		}
	}

	/**
	 * 向缓存中设置字符串内容 hset
	 * 
	 * @param field
	 * @param key
	 * @param value
	 * @return
	 * @throws Exception
	 */
	public static boolean hset(String field, String key, String value) throws Exception {
		try {
			JedisCluster jedisCluster = getJedisCluster();
			jedisCluster.hset(field.getBytes(), key.getBytes(), serialize(value));
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	/**
	 * 向缓存中获取字符串内容 hget
	 * 
	 * @param field
	 * @param key
	 * @return
	 * @throws Exception
	 */
	public static Object hget(String field, String key) throws Exception {
		Object value = null;
		try {
			JedisCluster jedisCluster = getJedisCluster();
			value = unserialize(jedisCluster.hget(field.getBytes(), key.getBytes()));
			return value;
		} catch (Exception e) {
			return value;
		}
	}

	/**
	 * 删除 hset中的数据
	 * 
	 * @param field
	 * @param key
	 * @return
	 * @throws Exception
	 */
	public static boolean hdel(String field, String key) throws Exception {
		try {
			JedisCluster jedisCluster = getJedisCluster();
			jedisCluster.hdel(field.getBytes(), key.getBytes());
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {
		}
	}

	/**
	 * 判断key是否存在
	 * @param key
	 * @return
	 * 
	 */
	public static Boolean exists(String key) {
		JedisCluster jedisCluster = getJedisCluster();
		return jedisCluster.exists(key);
	}

	/**
	 * 设置过期时间
	 *
	 * @param key
	 * @param seconds
	 */
	public static void expire(String key, int seconds) {
		if (seconds <= 0) {
			return;
		}
		JedisCluster jedis = getJedisCluster();
		jedis.expire(key, seconds);
	}

	/**
	 * @param pattern
	 *	通配符 返回与 pattern 相匹配的key 输入11 将返回 11*的key
	 * 
	 */
	public static List<String> getRediskeys(String pattern) {
		List<String> list = new LinkedList<>();
		JedisCluster jedisCluster = getJedisCluster();
		Set<HostAndPort> nodes = new HashSet<HostAndPort>();
		String osName = System.getProperty("os.name");

		String[] hosts	  = dev_hostAndPorts.split("\\|\\|");
		
		for (String hostport : hosts) {
			String[] ipport = hostport.split(":");
			String ip = ipport[0];
			int port = Integer.parseInt(ipport[1]);
			nodes.add(new HostAndPort(ip, port));
		}
		try {
			Map<String, JedisPool> clusterNodes = jedisCluster.getClusterNodes();
			for (Map.Entry<String, JedisPool> entry : clusterNodes.entrySet()) {
				Jedis jedis = entry.getValue().getResource();
				// 判断非从节点(因为若主从复制，从节点会跟随主节点的变化而变化)
				if (!jedis.info("replication").contains("role:slave")) {
					Set<String> keys = jedis.keys(pattern + "*");
					if (keys.size() > 0) {
						for (String key : keys) {
							list.add(key);
						}
					}
				}
			}
		} finally {
		}

		return list;
	}

	/**
	 * @param pattern
	 * 通配符 删除pattern 相匹配的key 输入11 将删除 11*的key
	 */
	public static boolean delRediskeys(String pattern) {
		JedisCluster jedisCluster = getJedisCluster();
		Set<HostAndPort> nodes = new HashSet<HostAndPort>();
	
		String osName = System.getProperty("os.name");
		String[]	 hosts = dev_hostAndPorts.split("\\|\\|");
		for (String hostport : hosts) {
			String[] ipport = hostport.split(":");
			String ip = ipport[0];
			int port = Integer.parseInt(ipport[1]);
			nodes.add(new HostAndPort(ip, port));
		}
		try {
			Map<String, JedisPool> clusterNodes = jedisCluster.getClusterNodes();
			for (Map.Entry<String, JedisPool> entry : clusterNodes.entrySet()) {
				Jedis jedis = entry.getValue().getResource();
				// 判断非从节点(因为若主从复制，从节点会跟随主节点的变化而变化)
				if (!jedis.info("replication").contains("role:slave")) {
					Set<String> keys = jedis.keys(pattern + "*");
					if (keys.size() > 0) {
						Map<Integer, List<String>> map = new HashMap<>();
						for (String key : keys) {
							// cluster模式执行多key操作的时候，这些key必须在同一个slot上，不然会报:JedisDataException:
							// CROSSSLOT Keys in request don't hash to the same slot
							int slot = JedisClusterCRC16.getSlot(key);
							// 按slot将key分组，相同slot的key一起提交
							if (map.containsKey(slot)) {
								map.get(slot).add(key);
							} else {
								map.put(slot, Lists.newArrayList(key));
							}
						}
						for (Map.Entry<Integer, List<String>> integerListEntry : map.entrySet()) {
							jedis.del(integerListEntry.getValue()
									.toArray(new String[integerListEntry.getValue().size()]));
						}
					}

				}
			}
			System.out.println("success deleted redisKeyStartWith:" + pattern.toString());
			return true;
		} catch (Exception e) {
			return false;
		} finally {
		}
	}

	// 序列化
	public static byte[] serialize(Object obj) {
		ObjectOutputStream obi = null;
		ByteArrayOutputStream bai = null;
		try {
			bai = new ByteArrayOutputStream();
			obi = new ObjectOutputStream(bai);
			obi.writeObject(obj);
			byte[] byt = bai.toByteArray();
			return byt;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	// 反序列化
	public static Object unserialize(byte[] byt) {
		if (byt == null) {
			return null;
		}
		ObjectInputStream oii = null;
		ByteArrayInputStream bis = null;
		bis = new ByteArrayInputStream(byt);
		try {
			oii = new ObjectInputStream(bis);
			Object obj = oii.readObject();
			return obj;
		} catch (Exception e) {

			e.printStackTrace();
		}
		return null;
	}

	public static void main(String[] args) {

		 Map<String, Object> map = new HashMap<>();
		 map.put("aa-2", "123");
		 set("name-5我擦", map);
		 System.out.println(get("name-5我擦"));
		
//		//1.random生成随机数
//			Random r = new Random();
//			//2.hashSet存储结果
//			HashSet<Integer> hs = new HashSet<Integer>();
//			//3.size>10时，停止放数
//			while(hs.size()<10){
//				//4.生成随机数
//				int res = r.nextInt(20)+1;
//				//5.添加到集合
//				hs.add(res);
//			}
//			System.out.println(hs);

		 // System.out.println(del("name-5我擦"));
//		 System.out.println(get("name-5我擦"));
//		 Map<String, Object> map2 = (Map<String, Object>) get("name-5我擦");
//		 System.out.println(map2.get("aa-2"));
//		 UUID uid = UUID.randomUUID();
//		 String token = uid.toString();
//		 setsession(token,"NX",map,200);
//		 System.out.println(token);
//		 System.out.println(((Map<String, Object>)get(token)).get("aa-2"));
//		 setsession(token,"XX",map,20);
//		 System.out.println(get(token));
//	 System.out.println(exists("name-5我擦"));
//		
//		 expire("name-5我擦",3);
//		 try {
//		 Thread.sleep(2000);
//		 } catch (InterruptedException e) {
//		 e.printStackTrace();
//		 }
//		 System.out.println(get("name-5我擦"));
//	
//		System.out.println(delRediskeys("11-"));
//		System.out.println(getRediskeys("11-"));
	}


}
// 单机版redis 集群出问题时 备选方案
//package com.jzy.edu.cloud.common;
//
//import java.io.ByteArrayInputStream;
//import java.io.ByteArrayOutputStream;
//import java.io.IOException;
//import java.io.ObjectInputStream;
//import java.io.ObjectOutputStream;
//import java.io.Serializable;
//
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//
//import com.alibaba.fastjson.JSON;
//
//import redis.clients.jedis.Jedis;
//import redis.clients.jedis.JedisPool;
//
///**
// * redis 缓存配置
// * 
// * @author yclimb
// */
//
//public class RedisCache implements Serializable {
//    /**
//     * 日志记录
//     */
//    private static final Log LOG = LogFactory.getLog(RedisCache.class);
//
//    /**
//     * redis 连接池
//     */
//    private static JedisPool pool;
//
//    public void setPool(JedisPool pool) {
//        this.pool = pool;
//    }
//
//    /*
//     * static { if (pool == null) { //读取相关的配置 ResourceBundle resourceBundle =
//     * ResourceBundle.getBundle("redis"); int maxActive =
//     * Integer.parseInt(resourceBundle.getString("redis.maxActive")); int maxIdle =
//     * Integer.parseInt(resourceBundle.getString("redis.maxIdle")); int maxWait =
//     * Integer.parseInt(resourceBundle.getString("redis.maxWait"));
//     * 
//     * String host = resourceBundle.getString("redis.host"); int port =
//     * Integer.parseInt(resourceBundle.getString("redis.port")); String pass =
//     * resourceBundle.getString("redis.pass");
//     * 
//     * JedisPoolConfig config = new JedisPoolConfig(); //设置最大连接数 config.setMaxTotal(maxActive);
//     * //设置最大空闲数 config.setMaxIdle(maxIdle); //设置超时时间 config.setMaxWaitMillis(maxWait);
//     * 
//     * //初始化连接池 pool = new JedisPool(config, host, port, 2000, pass); } }
//     */
//
//    /**
//     * 获取jedis
//     *
//     * @return jedis
//     */
//    public Jedis getResource() {
//        Jedis jedis = null;
//        try {
//            jedis = pool.getResource();
//        } catch (Exception e) {
//            LOG.info("can't get the redis resource");
//        }
//        return jedis;
//    }
//
//    /**
//     * 关闭连接
//     *
//     * @param jedis j
//     */
//    public void disconnect(Jedis jedis) {
//        jedis.disconnect();
//    }
//
//    /**
//     * 将jedis 返还连接池
//     *
//     * @param jedis j
//     */
//    public void returnResource(Jedis jedis) {
//        if (null != jedis) {
//            try {
//                pool.returnResource(jedis);
//            } catch (Exception e) {
//                LOG.info("can't return jedis to jedisPool");
//            }
//        }
//    }
//
//    /**
//     * 无法返还jedispool，释放jedis客户端对象
//     *
//     * @param jedis j
//     */
//    public void brokenResource(Jedis jedis) {
//        if (jedis != null) {
//            try {
//                pool.returnBrokenResource(jedis);
//            } catch (Exception e) {
//                LOG.info("can't release jedis Object");
//            }
//        }
//    }
//
//    /**
//     * 向缓存中设置字符串内容
//     * 
//     * @param key key
//     * @param value value
//     * @return
//     * @throws Exception
//     */
//    public static boolean set(String key, String value) throws Exception {
//        Jedis jedis = null;
//        try {
//            jedis = pool.getResource();
//            jedis.set(key.getBytes(), serialize(value));
//            return true;
//        } catch (Exception e) {
//            e.printStackTrace();
//            return false;
//        } finally {
//            pool.returnResource(jedis);
//        }
//    }
//
//    /**
//     * 向缓存中设置字符串内容 hset
//     * 
//     * @param field
//     * @param key
//     * @param value
//     * @return
//     * @throws Exception
//     */
//    public static boolean hset(String field, String key, String value) throws Exception {
//        Jedis jedis = null;
//        try {
//            jedis = pool.getResource();
//            jedis.hset(field.getBytes(), key.getBytes(), serialize(value));
//            return true;
//        } catch (Exception e) {
//            e.printStackTrace();
//            return false;
//        } finally {
//            pool.returnResource(jedis);
//        }
//    }
//
//    /**
//     * 向缓存中获取字符串内容 hget
//     * 
//     * @param field
//     * @param key
//     * @return
//     * @throws Exception
//     */
//    public static Object hget(String field, String key) throws Exception {
//        Jedis jedis = null;
//        Object value = null;
//        try {
//            jedis = pool.getResource();
//            value = unserialize(jedis.hget(field.getBytes(), key.getBytes()));
//
//            return value;
//        } catch (Exception e) {
//            e.printStackTrace();
//            return false;
//        } finally {
//            pool.returnResource(jedis);
//        }
//    }
//
//    /**
//     * 删除 hset中的数据
//     * 
//     * @param field
//     * @param key
//     * @return
//     * @throws Exception
//     */
//    public static boolean hdel(String field, String key) throws Exception {
//        Jedis jedis = null;
//        try {
//            jedis = pool.getResource();
//            jedis.hdel(field.getBytes(), key.getBytes());
//            return true;
//        } catch (Exception e) {
//            e.printStackTrace();
//            return false;
//        } finally {
//            pool.returnResource(jedis);
//        }
//    }
//
//    /**
//     * 向缓存中设置对象
//     * 
//     * @param key
//     * @param value
//     * @return
//     */
//    public static boolean set(String key, Object value) {
//        Jedis jedis = null;
//        try {
//            jedis = pool.getResource();
//            jedis.set(key.getBytes(), serialize(value.toString()));
//            return true;
//        } catch (Exception e) {
//            e.printStackTrace();
//            return false;
//        } finally {
//            pool.returnResource(jedis);
//        }
//    }
//
//    /**
//     * 向缓存中设置对象和过期时间
//     * 
//     * @param key
//     * @param u 值
//     * @param type NX是不存在时才set， XX是存在时才set
//     * @return
//     */
//    public static boolean setsession(String key, String type, Object u, int time) {
//        Jedis jedis = null;
//        try {
//            jedis = pool.getResource();
//            jedis.set(key.getBytes(), serialize(u), type.getBytes(), "EX".getBytes(), time);// NX是不存在时才set，
//                                                                                            // XX是存在时才set，
//                                                                                            // EX是秒，PX是毫秒
//            return true;
//        } catch (Exception e) {
//            e.printStackTrace();
//            return false;
//        } finally {
//            pool.returnResource(jedis);
//        }
//    }
//
//
//    /**
//     * 删除缓存中得对象，根据key
//     * 
//     * @param key
//     * @return
//     */
//    public static boolean del(String key) {
//        Jedis jedis = null;
//        try {
//            jedis = pool.getResource();
//            jedis.del(key.getBytes());
//            return true;
//        } catch (Exception e) {
//            e.printStackTrace();
//            return false;
//        } finally {
//            pool.returnResource(jedis);
//        }
//    }
//
//    /**
//     * 根据key 获取内容
//     * 
//     * @param key
//     * @return
//     */
//    public static Object get(String key) {
//        Jedis jedis = null;
//        try {
//            jedis = pool.getResource();
//            Object value = null;
//            if (jedis.get(key.getBytes()) != null) {
//                value = unserialize(jedis.get(key.getBytes()));
//            }
//
//            return value;
//        } catch (Exception e) {
//            e.printStackTrace();
//            return false;
//        } finally {
//            pool.returnResource(jedis);
//        }
//    }
//
//    /**
//     * 根据key 获取对象
//     * 
//     * @param key
//     * @return
//     */
//    public static <T> T get(String key, Class<T> clazz) {
//        Jedis jedis = null;
//        try {
//            jedis = pool.getResource();
//            String value = unserialize(jedis.get(key.getBytes())).toString();
//            return JSON.parseObject(value, clazz);
//        } catch (Exception e) {
//            e.printStackTrace();
//            return null;
//        } finally {
//            pool.returnResource(jedis);
//        }
//    }
//
//    // 序列化
//    public static byte[] serialize(Object obj) {
//        ObjectOutputStream obi = null;
//        ByteArrayOutputStream bai = null;
//        try {
//            bai = new ByteArrayOutputStream();
//            obi = new ObjectOutputStream(bai);
//            obi.writeObject(obj);
//            byte[] byt = bai.toByteArray();
//            return byt;
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return null;
//    }
//
//    // 反序列化
//    public static Object unserialize(byte[] byt) {
//        if (byt == null) {
//            return null;
//        }
//        ObjectInputStream oii = null;
//        ByteArrayInputStream bis = null;
//        bis = new ByteArrayInputStream(byt);
//        try {
//            oii = new ObjectInputStream(bis);
//            Object obj = oii.readObject();
//            return obj;
//        } catch (Exception e) {
//
//            e.printStackTrace();
//        }
//
//
//        return null;
//    }
//}
//
