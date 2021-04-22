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

SSH Slurm Runner does not support password authentication yet. You must point it to a file containing a private key.

#### sbatch

Currently all `sbatch` configuration must currently happen in the job file.
SSH Slurm Runner does not offer any other way of configuring your batch jobs.
Please note that the job file must be present on the remote machine. SSH Slurm Runner will not copy it by itself.

#### CLI Usage

```bash
python3 -m ssh_slurm_runner test.job --host cluster.example.com --user myuser --keyfile ~/.ssh/privatekeyfile
```
