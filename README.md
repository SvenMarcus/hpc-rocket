## 🚀 HPC Rocket

[![Python application](https://github.com/SvenMarcus/ssh-slurm-runner/actions/workflows/python-app.yml/badge.svg)](https://github.com/SvenMarcus/ssh-slurm-runner/actions/workflows/python-app.yml)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=SvenMarcus_ssh-slurm-runner&metric=alert_status)](https://sonarcloud.io/dashboard?id=SvenMarcus_ssh-slurm-runner)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=SvenMarcus_ssh-slurm-runner&metric=coverage)](https://sonarcloud.io/dashboard?id=SvenMarcus_ssh-slurm-runner)
[![Python](pythonversions.svg)](https://python.org)

HPC Rocket is a tool to send slurm commands to a remote machine and monitor the job progress. It was primarily written to launch slurm jobs from a CI pipeline.

![](demo/hpc-rocket-demo.gif)

### Installation

You can get the latest version of HPC Rocket on PyPI:
```
python3 -m pip install hpc-rocket
```

### Authentication

HPC Rocket does support authentication via password and private key. Password authentication currently requires adding it to the config file. Therefore, authentication via private key is recommended. Interactive password input is planned for a future version.

### Slurm configuration

Currently all `sbatch` configuration must happen in the job file.
HPC Rocket does not offer any other way of configuring your batch jobs.

### Configuration file

HPC Rocket uses a configuration file in YAML format containing credentials to connect to the remote machine. Additionally it allows copying files to the remote machine, copying results back to the local machine (collecting) and eventually cleaning up copied or produced files.
Note that all paths in the configuration file must be relative paths.
On the local machine paths are evaluated from the current working directory, on the remote machine from the user's home directory.
If you want to overwrite existing files on the remote machine, make sure to specify the `overwrite` instruction for each file you would like to overwrite.
HPC Rocket will evaluate environment variables on the **LOCAL** machine in the form of `${VAR}` and `$VAR` when parsing the config file.

```yaml
host: cluster.example.com
user: myuser
private_keyfile: ~/.ssh/id_rsa

proxyjumps:
  - host: myproxy.example.com
    user: myproxy-user
    private_keyfile: ~/.ssh/proxy_keyfile

copy:
  - from: jobs/slurm.job
    to: slurm.job
    overwrite: true

  - from: bin/myexecutable
    to: myexecutable

collect:
  - from: remote_slurmresult.out
    to: local_slurmresult.out
    overwrite: true

clean:
  - slurm.job
  - myexecutable

sbatch: slurm.job
```

### Usage

#### Launching a job on the remote machine

Use the `launch` command to launch a job on the remote machine. You must provide a configuration file. The optional `--watch` flag makes `hpc-rocket` wait until your job is finished (defaults to `false`). The collection and cleaning steps in the configuration file are only executed if `--watch` is set.

```bash
python3 -m hpc-rocket launch --watch config.yml
```

#### Checking a job's status

If a job was launched without `--watch` you can still check its status using the `status` command.
You will need to provide a configuration file with connection data and a job ID to check.

```bash
python3 -m hpc-rocket status config.yml 12345
```

#### Monitoring a job until it finishes

Similar to the `status` command, `hpc-rocket` also provides the `watch` command to monitor a job's status continuously by entering a config file and a job id.

```bash
python3 -m hpc-rocket watch config.yml 12345
```

#### Canceling a running job

Jobs may also be canceled using the `cancel` command. Like the previous commands it accepts a config file and the id of a running job.

```bash
hpc-rocket cancel config.yml 12345
```

