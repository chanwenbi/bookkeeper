package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class BookieAdmin {

    static final Logger LOG = LoggerFactory.getLogger(BookieAdmin.class);

    private static boolean confirm(boolean isInteractive, boolean force, String promptMessage) {
        boolean confirm = false;
        if (!isInteractive) {
            // If non interactive and force is set, then delete old
            // data.
            confirm = force;
        } else {
            try {
                confirm = IOUtils.confirmPrompt(promptMessage);
            } catch (IOException e) {
                LOG.error("Exception when confirming '{}' : ", promptMessage, e);
            }
        }
        return confirm;
    }

    /**
     * Format the bookie server data
     *
     * @param conf
     *            ServerConfiguration
     * @param isInteractive
     *            Whether format should ask prompt for confirmation if old data
     *            exists or not.
     * @param force
     *            If non interactive and force is true, then old data will be
     *            removed without confirm prompt.
     * @return Returns true if the format is success else returns false
     */
    public static boolean format(ServerConfiguration conf,
            boolean isInteractive, boolean force) {
        File journalDir = conf.getJournalDir();
        if (journalDir.exists() && journalDir.isDirectory()
                && journalDir.list().length != 0) {
            if (!confirm(isInteractive, force, "Are you sure to format Bookie data..?")) {
                LOG.error("Bookie format aborted!!");
                return false;
            }
        }
        if (!cleanDir(journalDir)) {
            LOG.error("Formatting journal directory failed");
            return false;
        }

        File[] ledgerDirs = conf.getLedgerDirs();
        for (File dir : ledgerDirs) {
            if (!cleanDir(dir)) {
                LOG.error("Formatting ledger directory " + dir + " failed");
                return false;
            }
        }

        // Clean up index directories if they are separate from the ledger dirs
        File[] indexDirs = conf.getIndexDirs();
        if (null != indexDirs) {
            for (File dir : indexDirs) {
                if (!cleanDir(dir)) {
                    LOG.error("Formatting ledger directory " + dir + " failed");
                    return false;
                }
            }
        }

        LOG.info("Bookie format completed successfully");
        return true;
    }

    private static boolean cleanDir(File dir) {
        if (dir.exists()) {
            for (File child : dir.listFiles()) {
                boolean delete = FileUtils.deleteQuietly(child);
                if (!delete) {
                    LOG.error("Not able to delete " + child);
                    return false;
                }
            }
        } else if (!dir.mkdirs()) {
            LOG.error("Not able to create the directory " + dir);
            return false;
        }
        return true;
    }
}
