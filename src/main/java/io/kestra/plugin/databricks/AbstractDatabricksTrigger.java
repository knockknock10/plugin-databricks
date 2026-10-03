package io.kestra.plugin.databricks;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.ConfigLoader;
import com.databricks.sdk.core.DatabricksConfig;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@NoArgsConstructor
@Getter
public abstract class AbstractDatabricksTrigger extends AbstractTrigger {
    @Schema(title = "Databricks host")
    @PluginProperty(group = "connection")
    private Property<String> host;

    @Schema(title = "Databricks account identifier")
    @PluginProperty(group = "advanced")
    private Property<String> accountId;

    @Schema(title = "Databricks configuration file, use this if you don't want to configure each Databricks account properties one by one")
    @PluginProperty(group = "advanced")
    private Property<String> configFile;

    @PluginProperty(dynamic = true, group = "connection")
    @Schema(
        title = "Databricks authentication configuration",
        description = """
            This property allows to configure the authentication to Databricks, different properties should be set depending on the type of authentication and the cloud provider.
            All configuration options can also be set using the standard Databricks environment variables.
            Check the Databricks authentication guide for more information.
            """
    )
    private AbstractTask.AuthenticationConfig authentication;

    protected WorkspaceClient workspaceClient(RunContext runContext) throws IllegalVariableEvaluationException {
        DatabricksConfig cfg = new DatabricksConfig()
            .setHost(runContext.render(host).as(String.class).orElse(null))
            .setAccountId(runContext.render(accountId).as(String.class).orElse(null))
            .setConfigFile(runContext.render(configFile).as(String.class).orElse(null));

        if (authentication != null) {
            cfg.setAuthType(runContext.render(authentication.getAuthType()).as(String.class).orElse(null))
                .setToken(runContext.render(authentication.getToken()).as(String.class).orElse(null))
                .setUsername(runContext.render(authentication.getUsername()).as(String.class).orElse(null))
                .setPassword(runContext.render(authentication.getPassword()).as(String.class).orElse(null))
                .setClientId(runContext.render(authentication.getClientId()).as(String.class).orElse(null))
                .setClientSecret(runContext.render(authentication.getClientSecret()).as(String.class).orElse(null))
                .setGoogleCredentials(runContext.render(authentication.getGoogleCredentials()).as(String.class).orElse(null))
                .setGoogleServiceAccount(runContext.render(authentication.getGoogleServiceAccount()).as(String.class).orElse(null))
                .setAzureClientId(runContext.render(authentication.getAzureClientId()).as(String.class).orElse(null))
                .setAzureClientSecret(runContext.render(authentication.getAzureClientSecret()).as(String.class).orElse(null))
                .setAzureTenantId(runContext.render(authentication.getAzureTenantId()).as(String.class).orElse(null));
        }

        ConfigLoader.resolve(cfg);
        return new WorkspaceClient(cfg);
    }
}
