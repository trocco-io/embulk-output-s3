/*
 * Copyright 2015 Manabu Takayama, and the Embulk project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.embulk.output.s3;

import java.io.File;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.IllegalFormatException;
import java.util.List;
import java.util.Locale;

import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.util.Md5Utils;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Buffer;
import org.embulk.spi.Exec;
import org.embulk.spi.FileOutput;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.TransactionalFileOutput;
import org.embulk.util.config.TaskMapper;
import org.embulk.util.retryhelper.RetryExecutor;
import org.embulk.util.retryhelper.RetryGiveupException;
import org.embulk.util.retryhelper.Retryable;
import org.slf4j.Logger;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.embulk.util.aws.credentials.AwsCredentials;
import org.embulk.util.aws.credentials.AwsCredentialsTask;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class S3FileOutputPlugin
        implements FileOutputPlugin
{
    private static final Logger logger = LoggerFactory.getLogger(S3FileOutputPlugin.class);
    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory
            .builder()
            .addDefaultModules()
            .build();
    private static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();

    public interface PluginTask
            extends AwsCredentialsTask, Task
    {
        @Config("path_prefix")
        String getPathPrefix();

        @Config("file_ext")
        String getFileNameExtension();

        @Config("sequence_format")
        @ConfigDefault("\".%03d.%02d\"")
        String getSequenceFormat();

        @Config("bucket")
        String getBucket();

        @Config("endpoint")
        @ConfigDefault("null")
        Optional<String> getEndpoint();

        @Config("http_proxy")
        @ConfigDefault("null")
        Optional<HttpProxy> getHttpProxy();
        void setHttpProxy(Optional<HttpProxy> httpProxy);

        @Config("proxy_host")
        @ConfigDefault("null")
        Optional<String> getProxyHost();

        @Config("proxy_port")
        @ConfigDefault("null")
        Optional<Integer> getProxyPort();

        @Config("tmp_path")
        @ConfigDefault("null")
        Optional<String> getTempPath();

        @Config("tmp_path_prefix")
        @ConfigDefault("\"embulk-output-s3-\"")
        String getTempPathPrefix();

        @Config("canned_acl")
        @ConfigDefault("null")
        Optional<CannedAccessControlList> getCannedAccessControlList();

        @Config("region")
        @ConfigDefault("null")
        Optional<String> getRegion();

        @Config("multipart_upload")
        @ConfigDefault("null")
        Optional<MultipartUpload> getMultipartUpload();
    }

    public static class S3FileOutput
            implements FileOutput,
            TransactionalFileOutput
    {
        private final String bucket;
        private final String pathPrefix;
        private final String sequenceFormat;
        private final String fileNameExtension;
        private final String tempPathPrefix;
        private final Optional<CannedAccessControlList> cannedAccessControlListOptional;
        private final MultipartUpload multipartUpload;

        private int taskIndex;
        private int fileIndex;
        private AmazonS3 client;
        private OutputStream current;
        private Path tempFilePath;
        private String tempPath = null;
        private String multipartUploadId = null;

        private AmazonS3 newS3Client(final PluginTask task)
        {
            Optional<String> endpoint = task.getEndpoint();
            Optional<String> region = task.getRegion();

            final AmazonS3ClientBuilder builder = AmazonS3ClientBuilder
                    .standard()
                    .withCredentials(getCredentialsProvider(task))
                    .withClientConfiguration(getClientConfiguration(task));

            // Favor the `endpoint` configuration, then `region`, if both are absent then `s3.amazonaws.com` will be used.
            if (endpoint.isPresent()) {
                if (region.isPresent()) {
                    logger.warn("Either configure endpoint or region, " +
                            "if both is specified only the endpoint will be in effect.");
                }
                builder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint.get(), null));
            }
            else if (region.isPresent()) {
                builder.setRegion(region.get());
            }
            else {
                // This is to keep the AWS SDK upgrading to 1.11.x to be backward compatible with old configuration.
                //
                // On SDK 1.10.x, when neither endpoint nor region is set explicitly, the client's endpoint will be by
                // default `s3.amazonaws.com`. And for pre-Signature-V4, this will work fine as the bucket's region
                // will be resolved to the appropriate region on server (AWS) side.
                //
                // On SDK 1.11.x, a region will be computed on client side by AwsRegionProvider and the endpoint now will
                // be region-specific `<region>.s3.amazonaws.com` and might be the wrong one.
                //
                // So a default endpoint of `s3.amazonaws.com` when both endpoint and region configs are absent are
                // necessary to make old configurations won't suddenly break. The side effect is that this will render
                // AwsRegionProvider useless. And it's worth to note that Signature-V4 won't work with either versions with
                // no explicit region or endpoint as the region (inferrable from endpoint) are necessary for signing.
                builder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("s3.amazonaws.com", null));
            }

            builder.withForceGlobalBucketAccessEnabled(true);
            return builder.build();
        }

        private AWSCredentialsProvider getCredentialsProvider(PluginTask task)
        {
            return AwsCredentials.getAWSCredentialsProvider(task);
        }

        private ClientConfiguration getClientConfiguration(PluginTask task)
        {
            ClientConfiguration clientConfig = new ClientConfiguration();

            clientConfig.setMaxConnections(50); // SDK default: 50
            clientConfig.setSocketTimeout(8 * 60 * 1000); // SDK default: 50*1000
            clientConfig.setRetryPolicy(PredefinedRetryPolicies.NO_RETRY_POLICY);

            // set http proxy
            // backward compatibility
            if (task.getProxyHost().isPresent()) {
                logger.warn("Configuration with \"proxy_host\" is deprecated. Use \"http_proxy.host\" instead.");
                if (!task.getHttpProxy().isPresent()) {
                    ConfigMapper configMapper = CONFIG_MAPPER_FACTORY.createConfigMapper();
                    ConfigSource configSource = CONFIG_MAPPER_FACTORY.newConfigSource();
                    configSource.set("host", task.getProxyHost().get());
                    HttpProxy httpProxy = configMapper.map(configSource, HttpProxy.class);
                    task.setHttpProxy(Optional.of(httpProxy));
                }
                else {
                    HttpProxy httpProxy = task.getHttpProxy().get();
                    if (httpProxy.getHost().isEmpty()) {
                        httpProxy.setHost(task.getProxyHost().get());
                        task.setHttpProxy(Optional.of(httpProxy));
                    }
                }
            }

            if (task.getProxyPort().isPresent()) {
                logger.warn("Configuration with \"proxy_port\" is deprecated. Use \"http_proxy.port\" instead.");
                HttpProxy httpProxy = task.getHttpProxy().get();
                if (!httpProxy.getPort().isPresent()) {
                    httpProxy.setPort(task.getProxyPort());
                    task.setHttpProxy(Optional.of(httpProxy));
                }
            }

            if (task.getHttpProxy().isPresent()) {
                setHttpProxyInAwsClient(clientConfig, task.getHttpProxy().get());
            }

            return clientConfig;
        }

        private void setHttpProxyInAwsClient(ClientConfiguration clientConfig, HttpProxy httpProxy)
        {
            // host
            clientConfig.setProxyHost(httpProxy.getHost());

            // port
            if (httpProxy.getPort().isPresent()) {
                clientConfig.setProxyPort(httpProxy.getPort().get());
            }

            // https
            clientConfig.setProtocol(httpProxy.getHttps() ? Protocol.HTTPS : Protocol.HTTP);

            // user
            if (httpProxy.getUser().isPresent()) {
                clientConfig.setProxyUsername(httpProxy.getUser().get());
            }

            // password
            if (httpProxy.getPassword().isPresent()) {
                clientConfig.setProxyPassword(httpProxy.getPassword().get());
            }
        }

        public S3FileOutput(PluginTask task, int taskIndex)
        {
            this.taskIndex = taskIndex;
            this.client = newS3Client(task);
            this.bucket = task.getBucket();
            this.pathPrefix = task.getPathPrefix();
            this.sequenceFormat = task.getSequenceFormat();
            this.fileNameExtension = task.getFileNameExtension();
            this.tempPathPrefix = task.getTempPathPrefix();
            if (task.getTempPath().isPresent()) {
                this.tempPath = task.getTempPath().get();
            }
            this.cannedAccessControlListOptional = task.getCannedAccessControlList();
            this.multipartUpload = task.getMultipartUpload().orElse(null);
        }

        private static Path newTempFile(String tmpDir, String prefix)
                throws IOException
        {
            if (tmpDir == null) {
                return Files.createTempFile(prefix, null);
            }
            else {
                return Files.createTempFile(Paths.get(tmpDir), prefix, null);
            }
        }

        private void deleteTempFile()
        {
            if (tempFilePath == null) {
                return;
            }

            try {
                Files.delete(tempFilePath);
                tempFilePath = null;
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private String buildCurrentKey()
        {
            String sequence = String.format(sequenceFormat, taskIndex,
                    fileIndex);
            return pathPrefix + sequence + fileNameExtension;
        }

        private void multipartUploadOrPutFile(Path from, String key)
        {
            if (from == null) {
                return;
            }
            if (multipartUpload != null) {
                multipartUploadFile(from, key);
            }
            else {
                putFile(from, key);
            }
        }

        private void multipartUploadFile(Path from, String key)
        {
            ExecutorService executor = Executors.newFixedThreadPool(multipartUpload.maxThreads);
            try {
                executeMultipartUpload(from, key, executor);
            }
            finally {
                abortMultipartUploadIfNecessary(key, executor);
            }
        }

        private void executeMultipartUpload(Path from, String key, ExecutorService executor)
        {
            File file = from.toFile();
            long fileSize = file.length();
            long fileOffset = 0;
            long partSize = multipartUpload.partSize;
            int partNumber = 1;
            int totalParts = (int) (fileSize / partSize) + (fileSize % partSize == 0 ? 0 : 1);
            List<Future<PartETag>> partETags = new ArrayList<>();
            multipartUploadId = client.initiateMultipartUpload(
                    new InitiateMultipartUploadRequest(bucket, key)).getUploadId();
            for (; fileOffset < fileSize; fileOffset += partSize, partNumber++) {
                partETags.add(submitUploadPart(
                        key,
                        file,
                        fileSize,
                        fileOffset,
                        partSize,
                        partNumber,
                        totalParts,
                        executor));
            }
            client.completeMultipartUpload(
                    new CompleteMultipartUploadRequest(bucket, key, multipartUploadId, collect(partETags)));
            multipartUploadId = null; // Successfully completed
        }

        private Future<PartETag> submitUploadPart(
                String key,
                File file,
                long fileSize,
                long fileOffset,
                long partSize,
                int partNumber,
                int totalParts,
                ExecutorService executor)
        {
            return executor.submit(() -> new UploadPart(
                    key,
                    file,
                    fileSize,
                    fileOffset,
                    partSize,
                    partNumber,
                    totalParts).runInterruptible());
        }

        private class UploadPart implements Retryable<PartETag>
        {
            final RetryExecutor re = RetryExecutor.builder().withRetryLimit(multipartUpload.retryLimit).build();
            final DecimalFormat df = new DecimalFormat("#,###"); // Not thread safe
            final String key;
            final File file;
            final long fileSize;
            final long fileOffset;
            final long partSize;
            final int partNumber;
            final int totalParts;
            final boolean isLastPart;
            final String md5Digest;

            UploadPart(
                    String key,
                    File file,
                    long fileSize,
                    long fileOffset,
                    long partSize,
                    int partNumber,
                    int totalParts)
            {
                this.key = key;
                this.file = file;
                this.fileSize = fileSize;
                this.fileOffset = fileOffset;
                this.partSize = Math.min(partSize, fileSize - fileOffset);
                this.partNumber = partNumber;
                this.totalParts = totalParts;
                isLastPart = partNumber >= totalParts;
                md5Digest = md5AsBase64(file, fileOffset, partSize);
            }

            PartETag runInterruptible() throws InterruptedException, RetryGiveupException
            {
                logger.info("Uploading a part {} / {}."
                        + " bucket '{}', key '{}', upload id '{}'",
                        partNumber, totalParts,
                        bucket, key, multipartUploadId);
                PartETag partETag = re.runInterruptible(this);
                logger.info("Uploaded {} / {} bytes of the file."
                        + " entity tag '{}'",
                        df.format(fileOffset + partSize), df.format(fileSize),
                        partETag.getETag());
                return partETag;
            }

            @Override
            public PartETag call()
            {
                return uploadPart(key, file, fileOffset, partSize, partNumber, isLastPart, md5Digest);
            }

            @Override
            public boolean isRetryableException(Exception exception)
            {
                return exception instanceof AmazonS3Exception;
            }

            @Override
            public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait)
            {
                logger.info("An error occurred while uploading a part {} / {},"
                        + " will retry {} / {} after {} milliseconds.",
                        partNumber, totalParts,
                        retryCount, retryLimit, df.format(retryWait), exception);
            }

            @Override
            public void onGiveup(Exception firstException, Exception lastException)
            {
                logger.warn("An error occurred while uploading a part {} / {},"
                        + " give up on retries.",
                        partNumber, totalParts, lastException);
            }
        }

        private PartETag uploadPart(
                String key,
                File file,
                long fileOffset,
                long partSize,
                int partNumber,
                boolean isLastPart,
                String md5Digest)
        {
            return client.uploadPart(new UploadPartRequest()
                    .withBucketName(bucket)
                    .withKey(key)
                    .withUploadId(multipartUploadId)
                    .withFile(file)
                    .withFileOffset(fileOffset)
                    .withPartSize(partSize)
                    .withPartNumber(partNumber)
                    .withLastPart(isLastPart)
                    .withMD5Digest(md5Digest)).getPartETag();
        }

        private void abortMultipartUploadIfNecessary(String key, ExecutorService executor)
        {
            if (multipartUploadId == null) { // Successfully completed
                return;
            }
            try {
                abortMultipartUpload(key, executor);
                logger.info("Aborts a multipart upload."
                        + " bucket '{}', key '{}', upload id '{}'",
                        bucket, key, multipartUploadId);
            }
            catch (RuntimeException e) {
                logger.warn("An error occurred while aborting a multipart upload.", e);
                logger.warn("An incomplete multipart upload may remain."
                        + " bucket '{}', key '{}', upload id '{}'",
                        bucket, key, multipartUploadId);
            }
        }

        private void abortMultipartUpload(String key, ExecutorService executor)
        {
            executor.shutdownNow(); // Attempts to terminate if possible
            client.abortMultipartUpload(new AbortMultipartUploadRequest(bucket, key, multipartUploadId));
        }

        private void putFile(Path from, String key)
        {
            PutObjectRequest request = new PutObjectRequest(bucket, key, from.toFile());
            if (cannedAccessControlListOptional.isPresent()) {
                request.withCannedAcl(cannedAccessControlListOptional.get());
            }
            client.putObject(request);
        }

        private void closeCurrent()
        {
            if (current == null) {
                return;
            }

            try {
                multipartUploadOrPutFile(tempFilePath, buildCurrentKey());
                fileIndex++;
            }
            finally {
                try {
                    current.close();
                    current = null;
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
                finally {
                    deleteTempFile();
                }
            }
        }

        @Override
        public void nextFile()
        {
            closeCurrent();

            try {
                tempFilePath = newTempFile(tempPath, tempPathPrefix);

                logger.info("Writing S3 file '{}'", buildCurrentKey());

                current = Files.newOutputStream(tempFilePath);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void add(Buffer buffer)
        {
            if (current == null) {
                throw new IllegalStateException(
                        "nextFile() must be called before poll()");
            }

            try {
                current.write(buffer.array(), buffer.offset(), buffer.limit());
            }
            catch (IOException ex) {
                deleteTempFile();
                throw new RuntimeException(ex);
            }
            finally {
                buffer.release();
            }
        }

        @Override
        public void finish()
        {
            closeCurrent();
        }

        @Override
        public void close()
        {
            closeCurrent();
        }

        @Override
        public void abort()
        {
            deleteTempFile();
        }

        @Override
        public TaskReport commit()
        {
            TaskReport report = CONFIG_MAPPER_FACTORY.newTaskReport();
            return report;
        }

        private static String md5AsBase64(File file, long fileOffset, long partSize)
        {
            try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
                raf.seek(fileOffset);
                return Md5Utils.md5AsBase64(new FilePartInputStream(raf, partSize));
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private static class FilePartInputStream extends FilterInputStream
        {
            final long partSize;
            long partOffset;

            FilePartInputStream(RandomAccessFile raf, long partSize)
            {
                super(Channels.newInputStream(raf.getChannel()));
                this.partSize = partSize;
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException
            {
                if (partOffset >= partSize) {
                    return -1; // End of part reached
                }
                int bytesRead = super.read(b, off, (int) Math.min(len, partSize - partOffset));
                if (bytesRead <= -1) {
                    return -1; // End of file reached
                }
                partOffset += bytesRead;
                return bytesRead;
            }
        }

        private static <T> List<T> collect(List<Future<T>> futures)
        {
            return futures.stream().map(S3FileOutput::get).collect(Collectors.toList());
        }

        private static <T> T get(Future<T> future)
        {
            try {
                return future.get();
            }
            catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class MultipartUpload
    {
        @JsonProperty("part_size")
        public final long partSize;
        @JsonProperty("max_threads")
        public final int maxThreads;
        @JsonProperty("retry_limit")
        public final int retryLimit;

        @JsonCreator
        public MultipartUpload(
                @JsonProperty("part_size") String partSize,
                @JsonProperty("max_threads") Integer maxThreads,
                @JsonProperty("retry_limit") Integer retryLimit)
        {
            this.partSize = parseLong(partSize != null ? partSize : "5g");
            this.maxThreads = maxThreads != null ? maxThreads : 4;
            this.retryLimit = retryLimit != null ? retryLimit : 3;
        }

        private static final long K = 1024;
        private static final long M = K * K;
        private static final long G = M * K;

        private static long parseLong(String valueWithUnit)
        {
            final long value = Long.parseLong(valueWithUnit.replaceFirst("[^0-9]*$", ""));
            final String unit = valueWithUnit.replaceFirst("^[0-9]*", "").toLowerCase();
            return value * (unit.equals("g") ? G : unit.equals("m") ? M : unit.equals("k") ? K : 1);
        }
    }

    private void validateSequenceFormat(PluginTask task)
    {
        try {
            @SuppressWarnings("unused")
            String dontCare = String.format(Locale.ENGLISH,
                    task.getSequenceFormat(), 0, 0);
        }
        catch (IllegalFormatException ex) {
            throw new ConfigException(
                    "Invalid sequence_format: parameter for file output plugin",
                    ex);
        }
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, int taskCount,
            Control control)
    {
        final PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);

        validateSequenceFormat(task);

        return resume(task.toTaskSource(), taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource, int taskCount,
            Control control)
    {
        control.run(taskSource);
        return CONFIG_MAPPER_FACTORY.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource, int taskCount,
            List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TransactionalFileOutput open(TaskSource taskSource, int taskIndex)
    {
        final TaskMapper taskMapper = CONFIG_MAPPER_FACTORY.createTaskMapper();
        final PluginTask task = taskMapper.map(taskSource, PluginTask.class);

        return new S3FileOutput(task, taskIndex);
    }
}
