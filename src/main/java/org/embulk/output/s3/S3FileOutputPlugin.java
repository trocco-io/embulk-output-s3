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
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.IllegalFormatException;
import java.util.List;
import java.util.Locale;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.utils.Md5Utils;
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
import org.embulk.spi.FileOutput;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.TransactionalFileOutput;
import org.embulk.util.config.TaskMapper;
import org.embulk.util.retryhelper.RetryExecutor;
import org.embulk.util.retryhelper.RetryGiveupException;
import org.embulk.util.retryhelper.Retryable;
import org.slf4j.Logger;

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
        Optional<ObjectCannedACL> getCannedAccessControlList();

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
        private final Optional<ObjectCannedACL> cannedAccessControlListOptional;
        private final MultipartUpload multipartUpload;

        private int taskIndex;
        private int fileIndex;
        private S3Client client;
        private OutputStream current;
        private Path tempFilePath;
        private String tempPath = null;
        private String multipartUploadId = null;

        private S3Client newS3Client(final PluginTask task)
        {
            Optional<String> endpoint = task.getEndpoint();
            Optional<String> region = task.getRegion();

            final S3ClientBuilder builder = S3Client.builder()
                    .credentialsProvider(getCredentialsProvider(task))
                    .httpClientBuilder(getHttpClientBuilder(task));

            if (region.isPresent()) {
                builder.region(Region.of(region.get()));
            }

            if (endpoint.isPresent()) {
                builder.endpointOverride(URI.create(endpoint.get()));
            }

            builder.forcePathStyle(false);
            return builder.build();
        }

        private AwsCredentialsProvider getCredentialsProvider(PluginTask task)
        {
            return AwsCredentialsProviderV2.getAwsCredentialsProvider(task);
        }

        private ApacheHttpClient.Builder getHttpClientBuilder(PluginTask task)
        {
            ApacheHttpClient.Builder httpClientBuilder = ApacheHttpClient.builder();

            httpClientBuilder.maxConnections(50);
            httpClientBuilder.socketTimeout(Duration.ofMillis(8 * 60 * 1000));

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
                setHttpProxyInAwsClient(httpClientBuilder, task.getHttpProxy().get());
            }

            return httpClientBuilder;
        }

        private void setHttpProxyInAwsClient(ApacheHttpClient.Builder httpClientBuilder, HttpProxy httpProxy)
        {
            ProxyConfiguration.Builder proxyConfig = ProxyConfiguration.builder();

            // host
            proxyConfig.endpoint(URI.create(
                    (httpProxy.getHttps() ? "https://" : "http://") +
                    httpProxy.getHost() +
                    (httpProxy.getPort().isPresent() ? ":" + httpProxy.getPort().get() : "")
            ));

            // user
            if (httpProxy.getUser().isPresent()) {
                proxyConfig.username(httpProxy.getUser().get());
            }

            // password
            if (httpProxy.getPassword().isPresent()) {
                proxyConfig.password(httpProxy.getPassword().get());
            }

            httpClientBuilder.proxyConfiguration(proxyConfig.build());
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
            List<Future<CompletedPart>> partETags = new ArrayList<>();

            CreateMultipartUploadRequest.Builder createRequestBuilder = CreateMultipartUploadRequest.builder()
                    .bucket(bucket)
                    .key(key);

            if (cannedAccessControlListOptional.isPresent()) {
                createRequestBuilder.acl(cannedAccessControlListOptional.get());
            }

            multipartUploadId = client.createMultipartUpload(createRequestBuilder.build()).uploadId();

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

            List<CompletedPart> completedParts = collect(partETags);

            client.completeMultipartUpload(
                    CompleteMultipartUploadRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .uploadId(multipartUploadId)
                            .multipartUpload(CompletedMultipartUpload.builder()
                                    .parts(completedParts)
                                    .build())
                            .build());
            multipartUploadId = null; // Successfully completed
        }

        private Future<CompletedPart> submitUploadPart(
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

        private class UploadPart implements Retryable<CompletedPart>
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

            CompletedPart runInterruptible() throws InterruptedException, RetryGiveupException
            {
                logger.info("Uploading a part {} / {}."
                        + " bucket '{}', key '{}', upload id '{}'",
                        partNumber, totalParts,
                        bucket, key, multipartUploadId);
                CompletedPart completedPart = re.runInterruptible(this);
                logger.info("Uploaded {} / {} bytes of the file."
                        + " entity tag '{}'",
                        df.format(fileOffset + partSize), df.format(fileSize),
                        completedPart.eTag());
                return completedPart;
            }

            @Override
            public CompletedPart call()
            {
                return uploadPart(key, file, fileOffset, partSize, partNumber, md5Digest);
            }

            @Override
            public boolean isRetryableException(Exception exception)
            {
                return exception instanceof S3Exception;
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

        private CompletedPart uploadPart(
                String key,
                File file,
                long fileOffset,
                long partSize,
                int partNumber,
                String md5Digest)
        {
            try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
                raf.seek(fileOffset);
                byte[] buffer = new byte[(int) partSize];
                int bytesRead = raf.read(buffer);

                byte[] partData = new byte[bytesRead];
                System.arraycopy(buffer, 0, partData, 0, bytesRead);

                UploadPartResponse response = client.uploadPart(
                        UploadPartRequest.builder()
                                .bucket(bucket)
                                .key(key)
                                .uploadId(multipartUploadId)
                                .partNumber(partNumber)
                                .contentMD5(md5Digest)
                                .build(),
                        RequestBody.fromBytes(partData));

                return CompletedPart.builder()
                        .partNumber(partNumber)
                        .eTag(response.eTag())
                        .build();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
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
            client.abortMultipartUpload(
                    AbortMultipartUploadRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .uploadId(multipartUploadId)
                            .build());
        }

        private void putFile(Path from, String key)
        {
            PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(key);

            if (cannedAccessControlListOptional.isPresent()) {
                requestBuilder.acl(cannedAccessControlListOptional.get());
            }

            client.putObject(requestBuilder.build(), RequestBody.fromFile(from));
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
            if (client != null) {
                client.close();
            }
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
