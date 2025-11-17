/*
 * Copyright 2015 The Embulk project
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

import org.embulk.config.ConfigException;
import org.embulk.util.aws.credentials.AwsCredentialsTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.nio.file.Paths;
import java.util.Optional;

/**
 * A utility class to generate AWS SDK v2 AwsCredentialsProvider from Embulk's task-defining interface.
 * This is compatible with embulk-util-aws-credentials but uses AWS SDK v2.
 */
public abstract class AwsCredentialsProviderV2
{
    private AwsCredentialsProviderV2()
    {
        // No instantiation.
    }

    /**
     * Creates AWS SDK v2 AwsCredentialsProvider from entries in task definition.
     *
     * @param task An entry in Embulk's task defining interface
     * @return AwsCredentialsProvider created
     */
    public static AwsCredentialsProvider getAwsCredentialsProvider(AwsCredentialsTask task)
    {
        return getAwsCredentialsProvider("", task);
    }

    private static AwsCredentialsProvider getAwsCredentialsProvider(String prefix, AwsCredentialsTask task)
    {
        String authMethodOption = prefix + "auth_method";
        String sessionTokenOption = prefix + "session_token";
        String profileFileOption = prefix + "profile_file";
        String profileNameOption = prefix + "profile_name";
        String accessKeyIdOption = prefix + "access_key_id";
        String secretAccessKeyOption = prefix + "secret_access_key";
        String accountIdOption = prefix + "account_id";
        String roleNameOption = prefix + "role_name";
        String externalIdOption = prefix + "external_id";

        switch (task.getAuthMethod()) {
            case "basic":
                // for backward compatibility
                if (!task.getAccessKeyId().isPresent() && !task.getSecretAccessKey().isPresent()) {
                    log.warn("Both '{}' and '{}' are not set. Assuming that '{}: anonymous' option is set.",
                            accessKeyIdOption, secretAccessKeyOption, authMethodOption);
                    log.warn("If you intentionally use anonymous authentication, please set 'auth_method: anonymous' option.");
                    log.warn("This behavior will be removed in a future release.");
                    reject(task.getSessionToken(), sessionTokenOption);
                    reject(task.getProfileFile(), profileFileOption);
                    reject(task.getProfileName(), profileNameOption);
                    reject(task.getAccountId(), accountIdOption);
                    reject(task.getRoleName(), roleNameOption);
                    reject(task.getExternalId(), externalIdOption);
                    return AnonymousCredentialsProvider.create();
                }
                else {
                    reject(task.getSessionToken(), sessionTokenOption);
                    reject(task.getProfileFile(), profileFileOption);
                    reject(task.getProfileName(), profileNameOption);
                    reject(task.getExternalId(), externalIdOption);
                    reject(task.getAccountId(), accountIdOption);
                    reject(task.getRoleName(), roleNameOption);
                    final String accessKeyId = require(task.getAccessKeyId(), "'access_key_id', 'secret_access_key'");
                    final String secretAccessKey = require(task.getSecretAccessKey(), "'secret_access_key'");
                    return StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(accessKeyId, secretAccessKey)
                    );
                }

            case "env":
                reject(task.getAccessKeyId(), accessKeyIdOption);
                reject(task.getSecretAccessKey(), secretAccessKeyOption);
                reject(task.getSessionToken(), sessionTokenOption);
                reject(task.getProfileFile(), profileFileOption);
                reject(task.getProfileName(), profileNameOption);
                reject(task.getAccountId(), accountIdOption);
                reject(task.getRoleName(), roleNameOption);
                reject(task.getExternalId(), externalIdOption);
                return EnvironmentVariableCredentialsProvider.create();

            case "instance":
                reject(task.getAccessKeyId(), accessKeyIdOption);
                reject(task.getSecretAccessKey(), secretAccessKeyOption);
                reject(task.getSessionToken(), sessionTokenOption);
                reject(task.getProfileFile(), profileFileOption);
                reject(task.getProfileName(), profileNameOption);
                reject(task.getAccountId(), accountIdOption);
                reject(task.getRoleName(), roleNameOption);
                reject(task.getExternalId(), externalIdOption);
                return InstanceProfileCredentialsProvider.create();

            case "profile":
            {
                reject(task.getAccessKeyId(), accessKeyIdOption);
                reject(task.getSecretAccessKey(), secretAccessKeyOption);
                reject(task.getSessionToken(), sessionTokenOption);
                reject(task.getAccountId(), accountIdOption);
                reject(task.getRoleName(), roleNameOption);
                reject(task.getExternalId(), externalIdOption);

                String profileName = task.getProfileName().orElse("default");
                ProfileCredentialsProvider.Builder builder = ProfileCredentialsProvider.builder()
                        .profileName(profileName);

                if (task.getProfileFile().isPresent()) {
                    builder.profileFile(ProfileFile.builder()
                            .content(Paths.get(task.getProfileFile().get()))
                            .type(ProfileFile.Type.CREDENTIALS)
                            .build());
                }

                return builder.build();
            }

            case "properties":
                reject(task.getAccessKeyId(), accessKeyIdOption);
                reject(task.getSecretAccessKey(), secretAccessKeyOption);
                reject(task.getSessionToken(), sessionTokenOption);
                reject(task.getProfileFile(), profileFileOption);
                reject(task.getProfileName(), profileNameOption);
                reject(task.getAccountId(), accountIdOption);
                reject(task.getRoleName(), roleNameOption);
                reject(task.getExternalId(), externalIdOption);
                return SystemPropertyCredentialsProvider.create();

            case "anonymous":
                reject(task.getAccessKeyId(), accessKeyIdOption);
                reject(task.getSecretAccessKey(), secretAccessKeyOption);
                reject(task.getSessionToken(), sessionTokenOption);
                reject(task.getProfileFile(), profileFileOption);
                reject(task.getProfileName(), profileNameOption);
                reject(task.getAccountId(), accountIdOption);
                reject(task.getRoleName(), roleNameOption);
                reject(task.getExternalId(), externalIdOption);
                return AnonymousCredentialsProvider.create();

            case "session":
            {
                final String accessKeyId = require(task.getAccessKeyId(),
                        "'" + accessKeyIdOption + "', '" + secretAccessKeyOption + "', '" + sessionTokenOption + "'");
                final String secretAccessKey = require(task.getSecretAccessKey(),
                        "'" + secretAccessKeyOption + "', '" + sessionTokenOption + "'");
                final String sessionToken = require(task.getSessionToken(),
                        "'" + sessionTokenOption + "'");
                reject(task.getProfileFile(), profileFileOption);
                reject(task.getProfileName(), profileNameOption);
                reject(task.getAccountId(), accountIdOption);
                reject(task.getRoleName(), roleNameOption);
                reject(task.getExternalId(), externalIdOption);
                return StaticCredentialsProvider.create(
                        AwsSessionCredentials.create(accessKeyId, secretAccessKey, sessionToken)
                );
            }

            case "assume_role":
            {
                reject(task.getAccessKeyId(), accessKeyIdOption);
                reject(task.getSecretAccessKey(), secretAccessKeyOption);
                reject(task.getSessionToken(), sessionTokenOption);
                reject(task.getProfileFile(), profileFileOption);
                reject(task.getProfileName(), profileNameOption);
                final String accountId = require(task.getAccountId(),
                        "'" + accountIdOption + "'");
                final String roleName = require(task.getRoleName(),
                        "'" + roleNameOption + "'");
                final String externalId = require(task.getExternalId(),
                        "'" + externalIdOption + "'");
                final String arn = String.format(ARN_PATTERN, task.getArnPartition(), accountId, roleName);

                // Create STS client with default credentials provider chain
                StsClient stsClient = StsClient.builder()
                        .credentialsProvider(DefaultCredentialsProvider.create())
                        .build();

                AssumeRoleRequest assumeRoleRequest = AssumeRoleRequest.builder()
                        .roleArn(arn)
                        .roleSessionName(task.getSessionName())
                        .externalId(externalId)
                        .durationSeconds(task.getDurationInSeconds())
                        .build();

                return StsAssumeRoleCredentialsProvider.builder()
                        .stsClient(stsClient)
                        .refreshRequest(assumeRoleRequest)
                        .asyncCredentialUpdateEnabled(false)
                        .build();
            }

            case "default":
            {
                reject(task.getAccessKeyId(), accessKeyIdOption);
                reject(task.getSecretAccessKey(), secretAccessKeyOption);
                reject(task.getSessionToken(), sessionTokenOption);
                reject(task.getProfileFile(), profileFileOption);
                reject(task.getProfileName(), profileNameOption);
                reject(task.getAccountId(), accountIdOption);
                reject(task.getRoleName(), roleNameOption);
                reject(task.getExternalId(), externalIdOption);
                return DefaultCredentialsProvider.create();
            }

            default:
                throw new ConfigException(String.format("Unknown auth_method '%s'. Supported methods are basic, env, instance, profile, properties, anonymous, session, assume_role and default.",
                        task.getAuthMethod()));
        }
    }

    private static <T> T require(Optional<T> value, String message)
    {
        if (value.isPresent()) {
            return value.get();
        }
        else {
            throw new ConfigException("Required option is not set: " + message);
        }
    }

    private static <T> void reject(Optional<T> value, String message)
    {
        if (value.isPresent()) {
            throw new ConfigException("Invalid option is set: " + message);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(AwsCredentialsProviderV2.class);
    private static final String ARN_PATTERN = "arn:%s:iam::%s:role/%s";
}
