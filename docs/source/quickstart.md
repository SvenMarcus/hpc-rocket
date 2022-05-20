# Command line usage

## Launching a job on the remote machine

Use the `launch` command to launch a job on the remote machine. You must provide a configuration file. The optional `--watch` flag makes `hpc-rocket` wait until your job is finished (defaults to `false`). The collection and cleaning steps in the configuration file are only executed if `--watch` is set.
Note the all Slurm configuration must happen in the slurm job submitted with `sbatch` file.
HPC Rocket does currently not offer any other way of configuring your batch jobs.

```bash
hpc-rocket launch --watch config.yml
```

## Checking a job's status

If a job was launched without `--watch` you can still check its status using the `status` command.
You will need to provide a configuration file with connection data and a job ID to check.

```bash
hpc-rocket status config.yml 12345
```

## Monitoring a job until it finishes

Similar to the `status` command, `hpc-rocket` also provides the `watch` command to monitor a job's status continuously by entering a config file and a job id.

```bash
hpc-rocket watch config.yml 12345
```

## Canceling a running job

Jobs may also be canceled using the `cancel` command. Like the previous commands it accepts a config file and the id of a running job.

```bash
hpc-rocket cancel config.yml 12345
```

