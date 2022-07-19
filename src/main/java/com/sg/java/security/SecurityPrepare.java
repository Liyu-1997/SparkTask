package com.sg.java.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class SecurityPrepare {

    private static final String USER_KEYTAB_FILE = "kerberos/user.keytab";

    private static final String USER_PRINCIPAL = "cq_qkjdyjc";

    /**
     * 这个是在项目工作目录源码文件夹下找绝对路径的配置文件的，这个需要源码文件和运行环境在一台机器上，不灵活，暂时不用
     */
    private static final String ABS_RESOURCE_PATH = System.getProperty("user.dir") + File.separator +
                                                    "src" + File.separator + "main" + File.separator + "resources" + File.separator;

    private static final Logger log = LoggerFactory.getLogger(SecurityPrepare.class);

    public static void securityPrepare() throws IOException {
        String krbFile = ABS_RESOURCE_PATH + "kerberos/krb5.conf";
        log.info("krb5 path：{}", krbFile);
        String userKeyTableFile = ABS_RESOURCE_PATH + USER_KEYTAB_FILE;
        log.info("keytab path：{}", userKeyTableFile);
        LoginUtil.setKrb5Config(krbFile);
        LoginUtil.setZookeeperServerPrincipal("zookeeper/hadoop.hadoop.com");
        LoginUtil.setJaasFile(USER_PRINCIPAL, userKeyTableFile);
    }

    public static void kerberosLogin() {
        try {
            securityPrepare();
            log.info("安全模式启动成功");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * kerberos安全认证相关文件已经放置于重庆那台ecs /qkjdy/resource/目录下
     */
    public static void cqEcsKerberosLogin() {
        System.setProperty("java.security.krb5.conf", "/qkjdy/resource/krb5.conf");
        System.setProperty("zookeeper.server.principal", "zookeeper/hadoop.hadoop.com");
        System.setProperty("java.security.auth.login.config", "/qkjdy/resource/client-user.jaas.conf");
        log.info("java.security.krb5.conf=/qkjdy/resource/krb5.conf");
        log.info("java.security.auth.login.config=/qkjdy/resource/client-user.jaas.conf");
    }

}
