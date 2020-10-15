package cn.hoob.structured_streaming.testForeachWriter;

import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.Serializable;

/**
 * @author zhuqinhe
 */
public class TestRedisForeachWriter extends ForeachWriter implements Serializable {

       public static JedisPool jedisPool;
        public Jedis jedis;
        static {
                JedisPoolConfig config = new JedisPoolConfig();
                config.setMaxTotal(20);
                config.setMaxIdle(5);
                config.setMaxWaitMillis(1000);
                config.setMinIdle(2);
                config.setTestOnBorrow(false);
                jedisPool = new JedisPool(config, "127.0.0.1", 6379);
        }

        public static synchronized Jedis getJedis() {
            return jedisPool.getResource();
        }


        @Override
        public boolean open(long partitionId, long version) {
           jedis = getJedis();
            return true;
        }

        @Override
        public void process(Object value) {
            GenericRowWithSchema genericRowWithSchema = (GenericRowWithSchema) value;

           System.out.println(((GenericRowWithSchema) value).get(0).toString()+"-----------"+
                   ((GenericRowWithSchema) value).get(1).toString());
            StructType schema=genericRowWithSchema.schema();
            String[] names=schema.fieldNames();
            Object[] values=genericRowWithSchema.values();
            jedis.set(values[0].toString(),values[1].toString());
        }

        @Override
        public void close(Throwable errorOrNull) {

            jedis.close();
        }
    }