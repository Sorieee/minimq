package top.sorie.minimq.common.util;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

/**
 * 本机 IP 获取工具。
 *
 * <p>优先返回内网（site-local）IPv4，取不到时退化为任意非回环 IPv4，
 * 仍取不到则返回回环地址 {@code 127.0.0.1}，保证调用方永远拿到可用的字符串。</p>
 */
public final class IpUtil {

    /** 拿不到任何网卡地址时的兜底值 */
    public static final String LOOPBACK_IP = "127.0.0.1";

    /**
     * 虚拟网卡关键字：容器、虚拟机、VPN 常驻，按网卡名与显示名匹配后跳过。
     * Windows 上 getName() 形如 ethernet_32779，虚拟化信息只在 getDisplayName() 里。
     */
    private static final String[] VIRTUAL_NIC_KEYWORDS = {
            "docker", "veth", "vethernet", "hyper-v", "virtual", "vmnet", "vboxnet",
            "virtualbox", "host-only", "virbr", "br-", "utun", "tun", "tap",
            "tailscale", "clash", "zerotier", "wsl"
    };

    private IpUtil() {
        // 工具类不允许实例化
    }

    /**
     * 获取本机 IP（优先内网 IPv4）。
     *
     * @return 本机 IP，取不到时返回 {@link #LOOPBACK_IP}
     */
    public static String getLocalIp() {
        return getLocalIp(LOOPBACK_IP);
    }

    /**
     * 获取本机 IP，允许调用方自定义兜底值。
     *
     * @param fallback 所有网卡都取不到时返回的值，为 null 时使用 {@link #LOOPBACK_IP}
     * @return 本机 IP
     */
    public static String getLocalIp(String fallback) {
        String ip = selectIpv4();
        if (ip != null) {
            return ip;
        }
        return fallback == null ? LOOPBACK_IP : fallback;
    }

    /**
     * 遍历所有网卡挑选 IPv4：内网地址直接返回，其它非回环地址作为候补。
     *
     * @return 选中的 IPv4，没有可用地址时返回 null
     */
    private static String selectIpv4() {
        String candidate = null;
        try {
            Enumeration<NetworkInterface> nics = NetworkInterface.getNetworkInterfaces();
            if (nics == null) {
                return null;
            }
            while (nics.hasMoreElements()) {
                NetworkInterface nic = nics.nextElement();
                if (!isUsable(nic)) {
                    continue;
                }
                for (InterfaceAddress interfaceAddress : nic.getInterfaceAddresses()) {
                    InetAddress address = interfaceAddress.getAddress();
                    if (!(address instanceof Inet4Address) || address.isLoopbackAddress()
                            || address.isLinkLocalAddress()) {
                        continue;
                    }
                    String hostAddress = address.getHostAddress();
                    if (address.isSiteLocalAddress()) {
                        return hostAddress;
                    }
                    if (candidate == null) {
                        candidate = hostAddress;
                    }
                }
            }
        } catch (SocketException e) {
            // 网卡枚举失败（权限或系统限制），退回兜底地址，由调用方决定是否告警
            System.err.println("枚举网卡失败，无法获取本机 IP: " + e.getMessage());
        }
        return candidate;
    }

    /** 网卡是否可用：已启用、非回环、非虚拟网卡 */
    private static boolean isUsable(NetworkInterface nic) throws SocketException {
        if (!nic.isUp() || nic.isLoopback()) {
            return false;
        }
        String name = nic.getName().toLowerCase();
        String displayName = nic.getDisplayName() == null ? "" : nic.getDisplayName().toLowerCase();
        return !matchesVirtualNic(name) && !matchesVirtualNic(displayName);
    }

    private static boolean matchesVirtualNic(String nicText) {
        for (String keyword : VIRTUAL_NIC_KEYWORDS) {
            if (nicText.contains(keyword)) {
                return true;
            }
        }
        return false;
    }
}
