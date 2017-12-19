package com.rundeck.plugin

import com.dtolabs.rundeck.core.common.Framework
import com.dtolabs.rundeck.core.common.INodeSet
import com.dtolabs.rundeck.core.resources.ResourceModelSource
import com.dtolabs.rundeck.core.resources.ResourceModelSourceException
import com.dtolabs.rundeck.core.resources.SourceType
import com.dtolabs.rundeck.core.resources.WriteableModelSource
import com.dtolabs.rundeck.core.resources.format.ResourceFormatParser
import com.dtolabs.rundeck.core.resources.format.ResourceFormatParserException
import com.dtolabs.rundeck.core.resources.format.UnsupportedFormatException
import com.dtolabs.rundeck.plugins.scm.ScmPluginInvalidInput
import com.dtolabs.utils.Streams
import org.apache.log4j.Logger
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.api.PullResult
import org.eclipse.jgit.api.TransportCommand
import org.eclipse.jgit.api.Status;
import org.eclipse.jgit.lib.BranchConfig
import org.eclipse.jgit.lib.ConfigConstants
import org.eclipse.jgit.lib.Constants
import org.eclipse.jgit.lib.Repository
import org.eclipse.jgit.transport.RemoteRefUpdate
import org.eclipse.jgit.transport.TrackingRefUpdate
import org.eclipse.jgit.transport.URIish
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider
import org.eclipse.jgit.storage.file.FileRepositoryBuilder
import org.eclipse.jgit.util.FileUtils

import java.nio.file.Files


/**
 * Created by luistoledo on 12/18/17.
 */
class GitResourceModel implements ResourceModelSource , WriteableModelSource{
    static Logger logger = Logger.getLogger(GitResourceModel.class);

    private Properties configuration;
    private Framework framework;
    private boolean writable=false;


    public static final String REMOTE_NAME = "origin"
    Git git
    String branch
    String extension
    String fileName
    String gitURL
    String localPath
    Repository repo

    GitResourceModel(Properties configuration, Framework framework) {
        this.configuration = configuration
        this.framework = framework

        this.branch = configuration.getProperty(GitResourceModelFactory.GIT_BRANCH)
        this.extension=configuration.getProperty(GitResourceModelFactory.GIT_FORMAT_FILE)
        this.writable=Boolean.valueOf(configuration.getProperty(GitResourceModelFactory.WRITABLE))
        this.fileName=configuration.getProperty(GitResourceModelFactory.GIT_FILE)
        this.gitURL=configuration.getProperty(GitResourceModelFactory.GIT_URL)
        this.localPath=configuration.getProperty(GitResourceModelFactory.GIT_BASE_DIRECTORY)


        logger.info("writable: ${this.writable}")
    }

    Map<String, String> getSshConfig() {
        def config = [:]

        String strictHostKeyChecking=configuration.getProperty(GitResourceModelFactory.GIT_HOSTKEY_CHECKING)

        if (strictHostKeyChecking in ['yes', 'no']) {
            config['StrictHostKeyChecking'] = strictHostKeyChecking
        }
        config
    }

    @Override
    INodeSet getNodes() throws ResourceModelSourceException {

        InputStream remoteFile = getFile();

        final ResourceFormatParser parser;
        try {
            parser = getResourceFormatParser();
        } catch (UnsupportedFormatException e) {
            throw new ResourceModelSourceException(
                    "Response content type is not supported: " + extension, e);
        }
        try {
            return parser.parseDocument(remoteFile);
        } catch (ResourceFormatParserException e) {
            throw new ResourceModelSourceException(
                    "Error requesting Resource Model Source from S3, "
                            + "Content could not be parsed: "+e.getMessage(),e);
        }


        return null
    }


    private InputStream getFile() {

        File base = new File(localPath)

        if(!base){
            base.mkdir()
        }

        cloneOrCreate(base, this.gitURL)

        Status status = this.getStatusInternal(true)

        println("Added: " + status.getAdded());
        println("Changed: " + status.getChanged());
        println("Conflicting: " + status.getConflicting());
        println("ConflictingStageState: " + status.getConflictingStageState());
        println("IgnoredNotInIndex: " + status.getIgnoredNotInIndex());
        println("Missing: " + status.getMissing());
        println("Modified: " + status.getModified());
        println("Removed: " + status.getRemoved());
        println("Untracked: " + status.getUntracked());
        println("UntrackedFolders: " + status.getUntrackedFolders());

        File file = new File(localPath+"/"+fileName)

        //always perform a pull
        gitPull()

        return file.newInputStream()

    }


    private ResourceFormatParser getResourceFormatParser() throws UnsupportedFormatException {
        return framework.getResourceFormatParserService().getParserForMIMEType(getMimeType());
    }

    private String getMimeType(){
        if(extension.equalsIgnoreCase("yaml")){
            return "text/yaml";
        }
        if(extension.equalsIgnoreCase("json")){
            return "application/json";
        }
        return "application/xml";
    }

    protected void cloneOrCreate(File base, String url) throws Exception {
        if (base.isDirectory() && new File(base, ".git").isDirectory()) {
            def arepo = new FileRepositoryBuilder().setGitDir(new File(base, ".git")).setWorkTree(base).build()
            def agit = new Git(arepo)

            //test url matches origin
            def config = agit.getRepository().getConfig()
            def found = config.getString("remote", REMOTE_NAME, "url")

            def needsClone=false;

            if (found != url) {
                logger.debug("url differs, re-cloning ${found}!=${url}")
                needsClone = true
            }else if (agit.repository.getFullBranch() != "refs/heads/$branch") {
                //check same branch
                logger.debug("branch differs, re-cloning")
                needsClone = true
            }

            if(needsClone){
                //need to reconfigured
                removeWorkdir(base)
                performClone(base, url)
                return
            }

            try {
                fetchFromRemote(agit)
            } catch (Exception e) {
                logger.debug("Failed fetch from the repository: ${e.message}", e)
                throw new Exception("Failed fetch from the repository: ${e.message}", e)
            }
            git = agit
            repo = arepo
        } else {
            performClone(base, url)
        }
    }

    private void removeWorkdir(File base) {
        //remove the dir
        FileUtils.delete(base, FileUtils.RECURSIVE)
    }

    def remoteTrackingBranch(Git git1 = null) {
        def agit = git1 ?: git

        def branchConfig = new BranchConfig(agit.repository.config, branch)
        return branchConfig.getRemoteTrackingBranch()
    }

    TrackingRefUpdate fetchFromRemote(Git git1 = null) {
        def agit = git1 ?: git
        def fetchCommand = agit.fetch()
        fetchCommand.setRemote(REMOTE_NAME)
        setupTransportAuthentication(sshConfig, fetchCommand)
        def fetchResult = fetchCommand.call()

        def update = fetchResult.getTrackingRefUpdate("refs/remotes/${REMOTE_NAME}/${this.branch}")

        def fetchMessage = update ? update.toString() : "No changes were found"
        getLogger().debug("fetchFromRemote: ${fetchMessage}")

        println("fetchFromRemote: ${fetchMessage}")
        //make sure tracking is configured for the branch
        if (!remoteTrackingBranch(agit)) {
            agit.repository.config.setString(
                    ConfigConstants.CONFIG_BRANCH_SECTION,
                    branch,
                    ConfigConstants.CONFIG_KEY_REMOTE,
                    REMOTE_NAME
            );
            //if remote branch name exists, track it for merging
            def remoteRef = fetchResult.getAdvertisedRef(branch) ?:
                    fetchResult.getAdvertisedRef(Constants.R_HEADS + branch)
            if (remoteRef) {
                agit.repository.config.setString(
                        ConfigConstants.CONFIG_BRANCH_SECTION,
                        branch,
                        ConfigConstants.CONFIG_KEY_MERGE,
                        remoteRef.name
                );
            }

            agit.repository.config.save()
        }

        return update
    }

    private void performClone(File base, String url) {
        logger.debug("cloning...");
        def cloneCommand = Git.cloneRepository().
                setBranch(this.branch).
                setRemote(REMOTE_NAME).
                setDirectory(base).
                setURI(url)
        setupTransportAuthentication(sshConfig, cloneCommand, url)
        try {
            git = cloneCommand.call()
        } catch (Exception e) {
            logger.debug("Failed cloning the repository from ${url}: ${e.message}", e)
            throw new Exception("Failed cloning the repository from ${url}: ${e.message}", e)
        }
        repo = git.getRepository()
    }

    void setupTransportAuthentication(
            Map<String, String> sshConfig,
            TransportCommand command,
            String url = null
    )
            throws Exception
    {
        if (!url) {
            url = command.repository.config.getString('remote', REMOTE_NAME, 'url')
        }
        if (!url) {
            throw new NullPointerException("url for remote was not set")
        }

        URIish u = new URIish(url);
        logger.debug("transport url ${u}, scheme ${u.scheme}, user ${u.user}")

        String sshPrivateKeyPath=configuration.getProperty(GitResourceModelFactory.GIT_KEY_STORAGE)
        String gitPasswordPath=configuration.getProperty(GitResourceModelFactory.GIT_PASSWORD_STORAGE)

        if ((u.scheme == null || u.scheme == 'ssh') && u.user && sshPrivateKeyPath) {
            logger.debug("using ssh private key path ${sshPrivateKeyPath}")
            //setup ssh key authentication
            //def keyData = sshPrivateKeyPath //TODO: open the file
            //def factory = new PluginSshSessionFactory(keyData)
            //factory.sshConfig = sshConfig
            //command.setTransportConfigCallback(factory)
        } else if (u.user && gitPasswordPath) {
            //setup password authentication
            logger.debug("using password")

            if (null != gitPasswordPath && gitPasswordPath.length() > 0) {
                command.setCredentialsProvider(new UsernamePasswordCredentialsProvider(u.user, gitPasswordPath))
            }
        }
    }

    PullResult gitPull(Git git1 = null) {
        def pullCommand = (git1 ?: git).pull().setRemote(REMOTE_NAME).setRemoteBranchName(branch)
        setupTransportAuthentication(sshConfig,pullCommand)
        pullCommand.call()
    }

    def gitCommitAndPush(){

        // run the add
        git.add()
                .addFilepattern(this.fileName)
                .call();

        // and then commit the changes
        git.commit()
                .setMessage("Edit node from GUI")
                .call();

        println("Committed file " + this.fileName + " to repository at " + repo.getDirectory());

        def pushb = git.push()
        pushb.setRemote(REMOTE_NAME)
        pushb.add(branch)
        setupTransportAuthentication(sshConfig, pushb)

        def push
        try {
            push = pushb.call()
        } catch (Exception e) {
            logger.debug("Failed push to remote: ${e.message}", e)
            throw new Exception("Failed push to remote: ${e.message}", e)
        }
        def sb = new StringBuilder()
        def updates = (push*.remoteUpdates).flatten()
        updates.each {
            sb.append it.toString()
        }

        String message=""
        def failedUpdates = updates.findAll { it.status != RemoteRefUpdate.Status.OK }
        if (failedUpdates) {
            message = "Some updates failed: " + failedUpdates
        } else {
            message = "Remote push result: OK."
        }

        logger.debug(message)

        println(message)

    }

    Status getStatusInternal(boolean performFetch) {
        //perform fetch
        def msgs=[]
        boolean fetchError=false
        if (performFetch) {
            try {
                fetchFromRemote()
            } catch (Exception e) {
                fetchError=true
                msgs<<"Fetch from the repository failed: ${e.message}"
                logger.error("Failed fetch from the repository: ${e.message}")
                logger.debug("Failed fetch from the repository: ${e.message}", e)
            }
        }

        Status status = git.status().call()

        return status
    }

    @Override
    public SourceType getSourceType() {
        return writable ? SourceType.READ_WRITE : SourceType.READ_ONLY;
    }

    @Override
    public WriteableModelSource getWriteable() {
        return writable ? this : null;
    }


    @Override
    public String getSyntaxMimeType() {
        try {
            return getResourceFormatParser().getPreferredMimeType();
        } catch (UnsupportedFormatException e) {
            e.printStackTrace()
        }
        return null
    }

    @Override
    long readData(OutputStream sink) throws IOException, ResourceModelSourceException {
        if (!hasData()) {
            return 0;
        }

        InputStream inputStream = getFile()

        return Streams.copyStream(inputStream, sink)
    }

    @Override
    boolean hasData() {
        try{
            getFile();
        }catch (Exception e){
            return false;
        }
        return true;
    }

    @Override
    public long writeData(InputStream data) throws IOException, ResourceModelSourceException {
        if (!writable) {
            throw new IllegalArgumentException("Cannot write to file, it is not configured to be writeable");
        }
        File newFile = isToFile(data);
        try {
            final INodeSet set = getResourceFormatParser().parseDocument(newFile);
        } catch (ResourceFormatParserException e) {
            throw new ResourceModelSourceException(e);
        }

        println "Editing the file"
        println newFile.getAbsolutePath()

        gitCommitAndPush()

        return newFile.length();
    }



    private File isToFile(InputStream is) throws IOException, ResourceModelSourceException {
        try {
            File newFile = new File(localPath+"/"+fileName)

            FileOutputStream fos = new FileOutputStream(newFile)
            Streams.copyStream(is, fos);
            return newFile;
        } catch (UnsupportedFormatException e) {
            throw new ResourceModelSourceException(
                    "Response content type is not supported: " + extension, e);
        }
    }

    @Override
    public String getSourceDescription() {
        return "Git repo: "+this.gitURL+", file:"+this.fileName;
    }
}
