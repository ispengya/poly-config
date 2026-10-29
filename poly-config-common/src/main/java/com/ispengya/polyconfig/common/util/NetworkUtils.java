package com.ispengya.polyconfig.common.util;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.Locale;

/**
 * 网络地址工具。
 */
public final class NetworkUtils {

    private NetworkUtils() {
    }

    /**
     * 解析节点对外展示的主机地址。
     *
     * <p>配置了具体地址时直接使用；配置为 0.0.0.0、localhost 或 127.x 时，
     * 自动探测本机非回环的 IPv4 地址，优先站点本地地址（如 192.168.x.x）。</p>
     *
     * @param configuredHost 配置的监听地址
     * @return 对外展示地址
     */
    public static String resolveAdvertisedHost(String configuredHost) {
        String host = configuredHost == null ? "" : configuredHost.trim();
        if (!host.isEmpty() && !isAnyOrLoopback(host)) {
            return host;
        }
        String detected = detectLanIpv4();
        if (detected != null) {
            return detected;
        }
        return "127.0.0.1";
    }

    private static boolean isAnyOrLoopback(String host) {
        String normalized = host.toLowerCase(Locale.ROOT);
        return "0.0.0.0".equals(normalized)
                || "::".equals(normalized)
                || "localhost".equals(normalized)
                || normalized.startsWith("127.");
    }

    private static String detectLanIpv4() {
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            String fallback = null;
            while (interfaces.hasMoreElements()) {
                NetworkInterface networkInterface = interfaces.nextElement();
                if (!networkInterface.isUp() || networkInterface.isLoopback()
                        || networkInterface.isVirtual()) {
                    continue;
                }
                Enumeration<InetAddress> addresses = networkInterface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress address = addresses.nextElement();
                    if (!(address instanceof Inet4Address)
                            || address.isLoopbackAddress()
                            || address.isLinkLocalAddress()
                            || address.isMulticastAddress()) {
                        continue;
                    }
                    if (address.isSiteLocalAddress()) {
                        return address.getHostAddress();
                    }
                    if (fallback == null) {
                        fallback = address.getHostAddress();
                    }
                }
            }
            return fallback;
        } catch (SocketException e) {
            return null;
        }
    }
}
