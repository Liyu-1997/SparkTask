package com.sg.java.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class SecurityPrepare {

    private static final String USER_KEYTAB_FILE = "user.keytab";

    private static final String USER_PRINCIPAL = "cq_qkjdyjc";

    private static final String ABS_RESOURCE_PATH = System.getProperty("user.dir") + File.separator +
                                                    "src" + File.separator + "main" + File.separator + "resources" + File.separator;

    private static final Logger log = LoggerFactory.getLogger(SecurityPrepare.class);

    public static void securityPrepare() throws IOException {
        String krbFile = ABS_RESOURCE_PATH + "krb5.conf";
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

}
