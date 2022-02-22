from airflow.plugins_manager import AirflowPlugin
from airflow.providers.ssh.hooks.ssh import SSHHook

class SSHHook2(SSHHook):
    def get_name2(self):
        return "SSHHook2"

class SSH2Plugin(AirflowPlugin):
    name = "ssh_plugin"
    hooks = [SSHHook2]
