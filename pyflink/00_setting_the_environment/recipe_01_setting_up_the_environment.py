from pyflink.table import EnvironmentSettings, TableEnvironment

# 1. Create a TableEnvironment for streaming mode
env_settings_streaming = EnvironmentSettings.in_streaming_mode()
t_env_streaming = TableEnvironment.create(env_settings_streaming)

# Or, for batch mode:
# env_settings_batch = EnvironmentSettings.in_batch_mode()
# t_env_batch = TableEnvironment.create(env_settings_batch)

# Set default parallelism (optional, useful for local testing)
t_env_streaming.get_config().set("parallelism.default", "1")
# t_env_batch.get_config().set("parallelism.default", "1")