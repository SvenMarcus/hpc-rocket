### SSH Slurm Runner

[![Python application](https://github.com/SvenMarcus/ssh-slurm-runner/actions/workflows/python-app.yml/badge.svg)](https://github.com/SvenMarcus/ssh-slurm-runner/actions/workflows/python-app.yml)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=SvenMarcus_ssh-slurm-runner&metric=alert_status)](https://sonarcloud.io/dashboard?id=SvenMarcus_ssh-slurm-runner)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=SvenMarcus_ssh-slurm-runner&metric=coverage)](https://sonarcloud.io/dashboard?id=SvenMarcus_ssh-slurm-runner)

SSH Slurm Runner utilizes [`paramiko`](http://www.paramiko.org) to send slurm commands to a remote machine and monitor the job progress. It was primarily written to launch slurm jobs from a CI pipeline.

#### Installation

Since this program is still in a very early stage it is currently not available on PyPi.

However you can still install it using the following command:

```bash
pip3 install git+git://github.com/SvenMarcus/ssh-slurm-runner 
```

#### Authentication

SSH Slurm Runner does support authentication via password and private key. Password authentication currently requires typing it in the command line directly or adding it to the config file. Therefore, authentication via private key is recommended. Interactive password input is planned for a future version.

#### sbatch

Currently all `sbatch` configuration must happen in the job file.
SSH Slurm Runner does not offer any other way of configuring your batch jobs.
If you're not using a config file the job file must be present on the remote machine.
The config file format allows specifying files to copy and clean.

### Configuration file

SSH Slurm Runner can use a configuration file in YAML format. An example is given below.
Note that all paths in the configuration file must be relative paths.
On the local machine paths are evaluated from the current working directory, on the remote machine from the user's home directory.

```yaml
host: cluster.example.com
user: myuser
private_keyfile: ~/.ssh/id_rsa

copy:
  - from: jobs/slurm.job
    to: slurm.job

clean:
  - slurm.job

sbatch: slurm.job
```


#### CLI Usage

With CLI configuration:

```bash
python3 -m ssh_slurm_runner run test.job --host cluster.example.com --user myuser --keyfile ~/.ssh/privatekeyfile
```

With configuration from file:
```bash
python3 -m ssh_slurm_runner from-config config.yml
```
