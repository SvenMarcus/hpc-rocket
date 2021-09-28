## HPC Rocket

[![Python application](https://github.com/SvenMarcus/ssh-slurm-runner/actions/workflows/python-app.yml/badge.svg)](https://github.com/SvenMarcus/ssh-slurm-runner/actions/workflows/python-app.yml)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=SvenMarcus_ssh-slurm-runner&metric=alert_status)](https://sonarcloud.io/dashboard?id=SvenMarcus_ssh-slurm-runner)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=SvenMarcus_ssh-slurm-runner&metric=coverage)](https://sonarcloud.io/dashboard?id=SvenMarcus_ssh-slurm-runner)
[![Python](pythonversions.svg)](https://python.org)

HPC Rocket utilizes [`paramiko`](http://www.paramiko.org) to send slurm commands to a remote machine and monitor the job progress. It was primarily written to launch slurm jobs from a CI pipeline.

### Installation

You can get the latest version of HPC Rocket on PyPI:
```
python3 -m pip install hpc-rocket
```


### Authentication

HPC Rocket does support authentication via password and private key. Password authentication currently requires typing it in the command line directly or adding it to the config file. Therefore, authentication via private key is recommended. Interactive password input is planned for a future version.

### sbatch

Currently all `sbatch` configuration must happen in the job file.
HPC Rocket does not offer any other way of configuring your batch jobs.
If you're not using a config file the job file must be present on the remote machine.
The config file format allows specifying files to copy, collect and clean.

### Configuration file

HPC Rocket can use a configuration file in YAML format. An example is given below.
This configuration file allows copying files to the remote machine, copying results back to the local machine (collecting) and eventually cleaning up copied or produced files.
Note that all paths in the configuration file must be relative paths.
On the local machine paths are evaluated from the current working directory, on the remote machine from the user's home directory.
If you want to overwrite existing files on the remote machine, make sure to specify the `overwrite` instruction for each file you would like to overwrite.

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

### CLI Usage

Since the yaml configuration offers a lot more options this is the recommended approach to running Slurm jobs. However, you can still run simple jobs with the CLI configuraition.

With configuration from file:
```bash
python3 -m hpc-rocket launch config.yml
```

With CLI configuration:

```bash
python3 -m hpc-rocket run test.job --host cluster.example.com --user myuser --keyfile ~/.ssh/privatekeyfile
```

