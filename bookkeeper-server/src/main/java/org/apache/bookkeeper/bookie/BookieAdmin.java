package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Charsets.UTF_8;

public class BookieAdmin {

    static final Logger LOG = LoggerFactory.getLogger(BookieAdmin.class);

    // Cookies

    /**
     * Check that the environment for the bookie is correct.
     * This means that the configuration has stayed the same as the
     * first run and the filesystem structure is up to date.
     */
    public static void checkEnvironment(ServerConfiguration conf,
                                        ZooKeeper zk, File journalDirectory,
                                        List<File> ledgerDirs, List<File> indexDirs) throws BookieException, IOException {
        List<File> allLedgerDirs = new ArrayList<File>(ledgerDirs.size() + indexDirs.size());
        allLedgerDirs.addAll(ledgerDirs);
        allLedgerDirs.addAll(indexDirs);
        if (zk == null) { // exists only for testing, just make sure directories are correct
            checkDirectoryStructure(journalDirectory);
            for (File dir : allLedgerDirs) {
                checkDirectoryStructure(dir);
            }
            return;
        }
        try {
            String instanceId = getInstanceId(zk, conf);
            boolean newEnv = false;
            Cookie masterCookie = Cookie.generateCookie(conf);
            if (null != instanceId) {
                masterCookie.setInstanceId(instanceId);
            }
            try {
                Cookie zkCookie = Cookie.readFromZooKeeper(zk, conf);
                masterCookie.verify(zkCookie);
            } catch (KeeperException.NoNodeException nne) {
                newEnv = true;
            }
            List<File> missedCookieDirs = new ArrayList<File>();
            checkDirectoryStructure(journalDirectory);

            // try to read cookie from journal directory
            try {
                Cookie journalCookie = Cookie.readFromDirectory(journalDirectory);
                journalCookie.verify(masterCookie);
            } catch (FileNotFoundException fnf) {
                missedCookieDirs.add(journalDirectory);
            }
            for (File dir : allLedgerDirs) {
                checkDirectoryStructure(dir);
                try {
                    Cookie c = Cookie.readFromDirectory(dir);
                    c.verify(masterCookie);
                } catch (FileNotFoundException fnf) {
                    missedCookieDirs.add(dir);
                }
            }

            if (!newEnv && missedCookieDirs.size() > 0){
                LOG.error("Cookie exists in zookeeper, but not in all local directories. "
                        + " Directories missing cookie file are " + missedCookieDirs);
                throw new BookieException.InvalidCookieException();
            }
            if (newEnv) {
                if (missedCookieDirs.size() > 0) {
                    LOG.debug("Directories missing cookie file are {}", missedCookieDirs);
                    masterCookie.writeToDirectory(journalDirectory);
                    for (File dir : allLedgerDirs) {
                        masterCookie.writeToDirectory(dir);
                    }
                }
                masterCookie.writeToZooKeeper(zk, conf);
            }
        } catch (KeeperException ke) {
            LOG.error("Couldn't access cookie in zookeeper", ke);
            throw new BookieException.InvalidCookieException(ke);
        } catch (UnknownHostException uhe) {
            LOG.error("Couldn't check cookies, networking is broken", uhe);
            throw new BookieException.InvalidCookieException(uhe);
        } catch (IOException ioe) {
            LOG.error("Error accessing cookie on disks", ioe);
            throw new BookieException.InvalidCookieException(ioe);
        } catch (InterruptedException ie) {
            LOG.error("Thread interrupted while checking cookies, exiting", ie);
            throw new BookieException.InvalidCookieException(ie);
        }
    }

    // Bookie Environments

    public static void checkDirectoryStructure(File dir) throws IOException {
        if (!dir.exists()) {
            File parent = dir.getParentFile();
            File preV3versionFile = new File(dir.getParent(),
                    BookKeeperConstants.VERSION_FILENAME);

            final AtomicBoolean oldDataExists = new AtomicBoolean(false);
            parent.list(new FilenameFilter() {
                    public boolean accept(File dir, String name) {
                        if (name.endsWith(".txn") || name.endsWith(".idx") || name.endsWith(".log")) {
                            oldDataExists.set(true);
                        }
                        return true;
                    }
                });
            if (preV3versionFile.exists() || oldDataExists.get()) {
                String err = "Directory layout version is less than 3, upgrade needed";
                LOG.error(err);
                throw new IOException(err);
            }
            if (!dir.mkdirs()) {
                String err = "Unable to create directory " + dir;
                LOG.error(err);
                throw new IOException(err);
            }
        }
    }

    public static String getInstanceId(ZooKeeper zk, ServerConfiguration conf) throws KeeperException,
            InterruptedException {
        String instanceId = null;
        if (zk.exists(conf.getZkLedgersRootPath(), null) == null) {
            LOG.error("BookKeeper metadata doesn't exist in zookeeper. "
                      + "Has the cluster been initialized? "
                      + "Try running bin/bookkeeper shell metaformat");
            throw new KeeperException.NoNodeException("BookKeeper metadata");
        }
        try {
            byte[] data = zk.getData(conf.getZkLedgersRootPath() + "/"
                    + BookKeeperConstants.INSTANCEID, false, null);
            instanceId = new String(data, UTF_8);
        } catch (KeeperException.NoNodeException e) {
            LOG.info("INSTANCEID not exists in zookeeper. Not considering it for data verification");
        }
        return instanceId;
    }

    // Bookie Formatter

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
