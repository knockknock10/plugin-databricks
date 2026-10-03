package io.kestra.plugin.databricks.dbfs;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.files.FileInfo;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.StatefulTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.databricks.AbstractTask;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import static io.kestra.core.models.triggers.StatefulTriggerService.computeAndUpdateState;
import static io.kestra.core.models.triggers.StatefulTriggerService.defaultKey;
import static io.kestra.core.models.triggers.StatefulTriggerService.readState;
import static io.kestra.core.models.triggers.StatefulTriggerService.writeState;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger on new or updated DBFS files and partitions",
    description = "Polls a DBFS path and triggers when files or partition directories are first discovered or changed. Detection state is persisted per path to avoid repeated triggering."
)
@Plugin(
    examples = {
        @Example(
            title = "Trigger on new files or partition directories in DBFS",
            full = true,
            code = """
                id: dbfs_listen
                namespace: company.team

                triggers:
                  - id: watch
                    type: io.kestra.plugin.databricks.dbfs.Trigger
                    interval: PT1M
                    host: "{{ secret('DATABRICKS_HOST') }}"
                    authentication:
                      token: "{{ secret('DATABRICKS_TOKEN') }}"
                    from: /mnt/incoming
                    recursive: true
                    includeDirectories: true
                    on: CREATE
                """
        )
    }
)
public class Trigger extends AbstractTask implements PollingTriggerInterface, TriggerOutput<Trigger.Output>, StatefulTriggerInterface {
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    @Schema(
        title = "Source DBFS path",
        description = "Absolute DBFS path to poll. When recursive is enabled, nested files and partition directories are included."
    )
    @NotNull
    @PluginProperty(group = "source")
    private Property<String> from;

    @Builder.Default
    @Schema(
        title = "Recursive",
        description = "Whether to recursively scan directories below the source path. Defaults to true."
    )
    private Property<Boolean> recursive = Property.ofValue(true);

    @Builder.Default
    @Schema(
        title = "Include directories",
        description = "Whether directory entries are treated as detectable partition events. Defaults to true."
    )
    private Property<Boolean> includeDirectories = Property.ofValue(true);

    @Builder.Default
    @Schema(
        title = "Trigger event type",
        description = "CREATE fires once for a newly discovered path, UPDATE fires when its size or modification time changes, and CREATE_OR_UPDATE fires for both."
    )
    private final Property<On> on = Property.ofValue(On.CREATE);

    @Schema(
        title = "State key",
        description = "Key used to persist per-path detection state; defaults to a stable per-trigger key."
    )
    @PluginProperty(group = "advanced")
    private Property<String> stateKey;

    @Schema(
        title = "State TTL",
        description = "Optional TTL for persisted detection state. Expired entries can trigger again when rediscovered."
    )
    @PluginProperty(group = "advanced")
    private Property<Duration> stateTtl;

    @Builder.Default
    @Schema(
        title = "Maximum files",
        description = "Maximum number of newly detected entries emitted by one trigger evaluation."
    )
    @PluginProperty(group = "execution")
    private Property<Integer> maxFiles = Property.ofValue(25);

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();

        String path = runContext.render(from)
            .as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("The from property is required."));

        if (!path.startsWith("/")) {
            throw new IllegalArgumentException("The from property must be an absolute DBFS path starting with '/'.");
        }

        int rMaxFiles = runContext.render(maxFiles).as(Integer.class).orElse(25);
        if (rMaxFiles < 1) {
            throw new IllegalArgumentException("The maxFiles property must be greater than 0.");
        }

        boolean rRecursive = runContext.render(recursive).as(Boolean.class).orElse(true);
        boolean rIncludeDirectories = runContext.render(includeDirectories).as(Boolean.class).orElse(true);
        On rOn = runContext.render(on).as(On.class).orElse(On.CREATE);
        String rStateKey = runContext.render(stateKey)
            .as(String.class)
            .orElse(defaultKey(context.getNamespace(), context.getFlowId(), id));
        Optional<Duration> rStateTtl = runContext.render(stateTtl).as(Duration.class);

        WorkspaceClient workspace = workspaceClient(runContext);
        Iterable<FileInfo> entries = rRecursive
            ? workspace.dbfs().recursiveList(path)
            : workspace.dbfs().list(path);

        var state = readState(runContext, rStateKey, rStateTtl);
        List<TriggeredFile> triggeredFiles = new ArrayList<>();

        for (FileInfo file : entries) {
            if (triggeredFiles.size() >= rMaxFiles) {
                break;
            }

            if (!isEligible(file, rIncludeDirectories)) {
                continue;
            }

            String filePath = file.getPath();
            String version = version(file);
            Instant modifiedAt = modifiedAt(file);

            var candidate = io.kestra.core.models.triggers.StatefulTriggerService.Entry.candidate(
                filePath,
                version,
                modifiedAt
            );
            var update = computeAndUpdateState(state, candidate, rOn);

            if (update.fire()) {
                triggeredFiles.add(
                    TriggeredFile.builder()
                        .file(file)
                        .changeType(update.isNew() ? ChangeType.CREATE : ChangeType.UPDATE)
                        .build()
                );
            }
        }

        writeState(runContext, rStateKey, state, rStateTtl);

        if (triggeredFiles.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(
            TriggerService.generateExecution(
                this,
                conditionContext,
                context,
                Output.builder().files(triggeredFiles).build()
            )
        );
    }

    private static boolean isEligible(FileInfo file, boolean includeDirectories) {
        if (file == null || file.getPath() == null || file.getPath().isBlank()) {
            return false;
        }

        return includeDirectories || !Boolean.TRUE.equals(file.getIsDir());
    }

    private static String version(FileInfo file) {
        long size = Optional.ofNullable(file.getFileSize()).orElse(0L);
        long modified = Optional.ofNullable(file.getModificationTime()).orElse(0L);
        boolean directory = Boolean.TRUE.equals(file.getIsDir());

        return String.format("directory:%s:size:%d:modified:%d", directory, size, modified);
    }

    private static Instant modifiedAt(FileInfo file) {
        long modified = Optional.ofNullable(file.getModificationTime()).orElse(0L);
        return Instant.ofEpochMilli(Math.max(0L, modified));
    }

    public enum ChangeType {
        CREATE,
        UPDATE
    }

    @Getter
    @AllArgsConstructor
    @Builder
    public static class TriggeredFile {
        @JsonUnwrapped
        private final FileInfo file;
        private final ChangeType changeType;

        public FileInfo toFile() {
            return file;
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "DBFS entries that triggered the flow, including files and optional partition directories")
        private final List<TriggeredFile> files;
    }
}
