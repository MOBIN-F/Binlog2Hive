package com.common;

import com.google.common.io.LineReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created with IDEA
 * Creater: MOBIN
 * Date: 2018/5/18
 * Time: 下午9:56
 */
public class ExecCMD {
    private static final Logger log = LoggerFactory.getLogger(ExecCMD.class);

    public static String exec(String cmd){
        try {
            String[] cmds = {"/bin/sh", "-c", cmd};
            Process process = Runtime.getRuntime().exec(cmds);
            LineReader lineReader = new LineReader(new InputStreamReader(process.getInputStream()));
            StringBuffer sb = new StringBuffer();
            String line;
            while ((line = lineReader.readLine()) != null){
                sb.append(line).append("\n");
            }
            String rs = sb.toString();
            if (!cmd.isEmpty())
                log.info("cmd executed, result: " + rs);
            return rs;
        } catch (IOException e) {
            log.error("Failed to exec cmd: " + cmd, e);
        }
        return null;
    }
}
