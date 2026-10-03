package io.kestra.plugin.databricks;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.mixin.DbfsExt;
import com.databricks.sdk.service.files.FileInfo;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.StatefulTriggerInterface;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@KestraTest
class DbfsTriggerTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void triggersNewFilesAndPartitionsOnlyOnce() throws Exception {
        WorkspaceClient workspace = mock(WorkspaceClient.class);
        DbfsExt dbfs = mock(DbfsExt.class);

        FileInfo partition = new FileInfo()
            .setPath("/mnt/events/date=2026-10-03")
            .setIsDir(true)
            .setModificationTime(1_000L)
            .setFileSize(0L);

        FileInfo file = new FileInfo()
            .setPath("/mnt/events/date=2026-10-03/data.json")
            .setIsDir(false)
            .setModificationTime(2_000L)
            .setFileSize(42L);

        when(workspace.dbfs()).thenReturn(dbfs);
        when(dbfs.recursiveList("/mnt/events")).thenReturn(List.of(partition, file));

        Trigger trigger = Trigger.builder()
            .id("dbfs-trigger")
            .type(Trigger.class.getName())
            .from(Property.ofValue("/mnt/events"))
            .interval(Duration.ofSeconds(10))
            .on(Property.ofValue(StatefulTriggerInterface.On.CREATE))
            .maxFiles(Property.ofValue(10))
            .build();

        doReturn(workspace).when(trigger).workspaceClient(org.mockito.ArgumentMatchers.any());

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.TriggerState> context =
            TestsUtils.mockTrigger(runContextFactory, trigger);

        Optional<Execution> first = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(first.isPresent(), is(true));

        @SuppressWarnings("unchecked")
        List<Trigger.TriggeredFile> files =
            (List<Trigger.TriggeredFile>) first.get().getTrigger().getVariables().get("files");

        assertThat(files, hasSize(2));
        assertThat(files.get(0).getFile().getPath(), is("/mnt/events/date=2026-10-03"));
        assertThat(files.get(0).getChangeType(), is(Trigger.ChangeType.CREATE));

        Optional<Execution> second = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(second.isPresent(), is(false));
    }

    @Test
    void detectsUpdatesWhenConfigured() throws Exception {
        WorkspaceClient workspace = mock(WorkspaceClient.class);
        DbfsExt dbfs = mock(DbfsExt.class);

        FileInfo file = new FileInfo()
            .setPath("/mnt/events/data.json")
            .setIsDir(false)
            .setModificationTime(1_000L)
            .setFileSize(42L);

        when(workspace.dbfs()).thenReturn(dbfs);
        when(dbfs.list("/mnt/events")).thenReturn(List.of(file));

        Trigger trigger = Trigger.builder()
            .id("dbfs-update-trigger")
            .type(Trigger.class.getName())
            .from(Property.ofValue("/mnt/events"))
            .recursive(Property.ofValue(false))
            .includeDirectories(Property.ofValue(false))
            .on(Property.ofValue(StatefulTriggerInterface.On.CREATE_OR_UPDATE))
            .build();

        doReturn(workspace).when(trigger).workspaceClient(org.mockito.ArgumentMatchers.any());

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.TriggerState> context =
            TestsUtils.mockTrigger(runContextFactory, trigger);

        Optional<Execution> first = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(first.isPresent(), is(true));

        file.setModificationTime(2_000L).setFileSize(84L);

        Optional<Execution> second = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(second.isPresent(), is(true));

        @SuppressWarnings("unchecked")
        List<Trigger.TriggeredFile> files =
            (List<Trigger.TriggeredFile>) second.get().getTrigger().getVariables().get("files");

        assertThat(files, hasSize(1));
        assertThat(files.get(0).getChangeType(), is(Trigger.ChangeType.UPDATE));
    }

    @Test
    void excludesDirectoriesWhenConfigured() throws Exception {
        WorkspaceClient workspace = mock(WorkspaceClient.class);
        DbfsExt dbfs = mock(DbfsExt.class);

        FileInfo directory = new FileInfo()
            .setPath("/mnt/events/date=2026-10-03")
            .setIsDir(true)
            .setModificationTime(1_000L);

        FileInfo file = new FileInfo()
            .setPath("/mnt/events/data.json")
            .setIsDir(false)
            .setModificationTime(2_000L)
            .setFileSize(42L);

        when(workspace.dbfs()).thenReturn(dbfs);
        when(dbfs.list("/mnt/events")).thenReturn(List.of(directory, file));

        Trigger trigger = Trigger.builder()
            .id("dbfs-files-only")
            .type(Trigger.class.getName())
            .from(Property.ofValue("/mnt/events"))
            .recursive(Property.ofValue(false))
            .includeDirectories(Property.ofValue(false))
            .build();

        doReturn(workspace).when(trigger).workspaceClient(org.mockito.ArgumentMatchers.any());

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.TriggerState> context =
            TestsUtils.mockTrigger(runContextFactory, trigger);

        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(execution.isPresent(), is(true));

        @SuppressWarnings("unchecked")
        List<Trigger.TriggeredFile> files =
            (List<Trigger.TriggeredFile>) execution.get().getTrigger().getVariables().get("files");

        assertThat(files, hasSize(1));
        assertThat(files.get(0).getFile().getPath(), is("/mnt/events/data.json"));
    }

    @Test
    void maxFilesDefersUnemittedEntriesToTheNextPoll() throws Exception {
        WorkspaceClient workspace = mock(WorkspaceClient.class);
        DbfsExt dbfs = mock(DbfsExt.class);

        FileInfo first = new FileInfo()
            .setPath("/mnt/events/1.json")
            .setIsDir(false)
            .setModificationTime(1_000L)
            .setFileSize(1L);

        FileInfo second = new FileInfo()
            .setPath("/mnt/events/2.json")
            .setIsDir(false)
            .setModificationTime(2_000L)
            .setFileSize(2L);

        FileInfo third = new FileInfo()
            .setPath("/mnt/events/3.json")
            .setIsDir(false)
            .setModificationTime(3_000L)
            .setFileSize(3L);

        when(workspace.dbfs()).thenReturn(dbfs);
        when(dbfs.list("/mnt/events")).thenReturn(List.of(first, second, third));

        Trigger trigger = Trigger.builder()
            .id("dbfs-max-files")
            .type(Trigger.class.getName())
            .from(Property.ofValue("/mnt/events"))
            .recursive(Property.ofValue(false))
            .includeDirectories(Property.ofValue(false))
            .maxFiles(Property.ofValue(2))
            .build();

        doReturn(workspace).when(trigger).workspaceClient(org.mockito.ArgumentMatchers.any());

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.TriggerState> context =
            TestsUtils.mockTrigger(runContextFactory, trigger);

        Optional<Execution> firstExecution = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(firstExecution.isPresent(), is(true));

        @SuppressWarnings("unchecked")
        List<Trigger.TriggeredFile> firstBatch =
            (List<Trigger.TriggeredFile>) firstExecution.get().getTrigger().getVariables().get("files");
        assertThat(firstBatch, hasSize(2));

        Optional<Execution> secondExecution = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(secondExecution.isPresent(), is(true));

        @SuppressWarnings("unchecked")
        List<Trigger.TriggeredFile> secondBatch =
            (List<Trigger.TriggeredFile>) secondExecution.get().getTrigger().getVariables().get("files");
        assertThat(secondBatch, hasSize(1));
        assertThat(secondBatch.get(0).getFile().getPath(), is("/mnt/events/3.json"));
    }

    @Test
    void rejectsRelativePath() {
        Trigger trigger = Trigger.builder()
            .id("dbfs-invalid-path")
            .type(Trigger.class.getName())
            .from(Property.ofValue("mnt/events"))
            .build();

        org.junit.jupiter.api.Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> {
                Map.Entry<ConditionContext, io.kestra.core.models.triggers.TriggerState> context =
                    TestsUtils.mockTrigger(runContextFactory, trigger);
                trigger.evaluate(context.getKey(), context.getValue());
            }
        );
    }

    @Test
    void rejectsInvalidMaxFiles() {
        Trigger trigger = Trigger.builder()
            .id("dbfs-invalid-max")
            .type(Trigger.class.getName())
            .from(Property.ofValue("/mnt/events"))
            .maxFiles(Property.ofValue(0))
            .build();

        org.junit.jupiter.api.Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> {
                Map.Entry<ConditionContext, io.kestra.core.models.triggers.TriggerState> context =
                    TestsUtils.mockTrigger(runContextFactory, trigger);
                trigger.evaluate(context.getKey(), context.getValue());
            }
        );
    }
}
