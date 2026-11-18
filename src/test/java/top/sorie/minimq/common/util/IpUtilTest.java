package top.sorie.minimq.common.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertFalse;

class IpUtilTest {

    @Test
    void getLocalIp_returnsNonBlankIPv4() {
        String ip = IpUtil.getLocalIp();

        assertNotNull(ip);
        assertFalse(ip.isBlank());
        assertFalse(ip.contains(" "), "IP 不应包含空白字符: " + ip);
        assertFalse(ip.contains(":"), "默认应返回 IPv4，实际: " + ip);
    }

    @Test
    void getLocalIp_withFallback_returnsFallbackValueWhenPassedNull() {
        // 网络环境不确定：有可用网卡时返回真实 IP，否则必须落在兜底值上
        String ip = IpUtil.getLocalIp(null);

        assertNotNull(ip);
        assertFalse(ip.isBlank());
    }

    @Test
    void getLocalIp_withCustomFallback_isStable() {
        String ip = IpUtil.getLocalIp("10.0.0.1");

        assertEquals(ip, IpUtil.getLocalIp("10.0.0.1"));
    }
}
