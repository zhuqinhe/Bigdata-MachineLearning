package cn.hoob.aceessprocess;

import org.apache.commons.lang3.StringUtils;
import scala.Tuple3;

import java.math.BigInteger;
import java.util.List;

public class IpUtils {
    /**
     * 将字符串的IP地址转为BigInteger
     * @param ip
     * @return
     */
    public static Long iptoInt(String ip){
        BigInteger big = BigInteger.ZERO;
        if(StringUtils.isEmpty(ip)) {
            return big.longValue();
        }
        try {
            if(ip.contains(".")){
                String[] ipNums = ip.split("\\.");
                for (int i = 0; i < ipNums.length; i++) {
                    big = big.add(new BigInteger(ipNums[i]).shiftLeft(8*(ipNums.length-i-1)));
                }
            }else if(ip.contains(":")){
                big = ipv6toInt(ip);

            }else{

                return big.longValue();
            }
        } catch (Exception e) {

        }
        return big.longValue();
    }



    private static BigInteger ipv6toInt(String ipv6){
        int compressIndex = ipv6.indexOf("::");
        if (compressIndex != -1){
            String part1s = ipv6.substring(0, compressIndex);
            String part2s = ipv6.substring(compressIndex + 1);
            BigInteger part1 = ipv6toInt(part1s);
            BigInteger part2 = ipv6toInt(part2s);
            int part1hasDot = 0;
            char ch[] = part1s.toCharArray();
            for (char c : ch){
                if (c == ':'){
                    part1hasDot++;
                }
            }
            return part1.shiftLeft(16 * (7 - part1hasDot )).add(part2);
        }
        String[] str = ipv6.split(":");
        BigInteger big = BigInteger.ZERO;
        for (int i = 0; i < str.length; i++){
            //::1
            if (str[i].isEmpty()){
                str[i] = "0";
            }
            big = big.add(BigInteger.valueOf(Long.valueOf(str[i], 16))
                    .shiftLeft(16 * (str.length - i - 1)));
        }
        return big;
    }
    public static Integer getProvince(Long ipLong, List<Tuple3<Long,Long,String>>rules){
     long min=0;
     long max=rules.size()-1;
     while(min<max){
        Integer midel= Math.toIntExact((min + max) / 2);
        if(ipLong>=rules.get(midel)._1()&&ipLong<=rules.get(midel)._2()){
            return midel;
        }
        if(ipLong<rules.get(midel)._1()){
            max=midel-1;
        }else{
            min=midel+1;
        }
     }
     return -1;
    }
}
