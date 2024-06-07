# Setting up a configuration file

Create a `YAML` file for the HPC Rocket configuration, e.g. `rocket.yaml` and open it.

## Connection data

Enter the connection data as shown below into the configuration file. For use in CI pipelines it is recommended to use environment variables, however it is also possible to fill the information in directly in the configuration file. HPC Rocket allows the use of a private key file or a password for authentication. Private key files are recommended. It is also possible to specify proxyjumps if necessary.

```yaml
host: $REMOTE_HOST
user: $REMOTE_USER
# Use private_keyfile OR password
private_keyfile: $PRIVATE_KEY
password: $PASSWORD

# Skip this section if you don't need proxyjumps
proxyjumps:
  - host: $PROXY_HOST
    user: $PROXY_USER
    private_keyfile: $PROXY_KEY 
```

## Copying files to the remote machine

Add all file you want to copy to the remote machine to the `copy` section. `from` refers to the location of a file on the local machine, `to` specifies the location on the remote machine the file will be copied to. If a file is already present on the remote machine the application will abort unless `overwrite: true` is set for a file.
This section must include the Slurm batch script to be run if it is not already present on the remote machine. HPC Rocket does support simple glob style notation (e.g. `folder/*.txt`).

```yaml
copy:
  - from: slurm_script.sh
    to: slurm_script.sh
    overwrite: true

  - from: important_file.txt
    to: important_file_remote.txt
    overwrite: true

  - from: myexecutable
    to: myexecutable

# ...
```

## Collecting files from the remote machine back to the local machine

Add all files you want to copy from the remote machine back to the local machine to the `collect` section. The same rules as in the `copy` sections apply, only that `from` now refers to the remote location and `to` specifies the local location of files. The `collect` step will be executed after the Slurm job was completed. Files will only be collected if the slurm job succeeds, unless `continue_if_job_fails` is set to `true` ([see `Specifying the Slurm Batch script`](#specifying-the-slurm-batch-script)).

```yaml
collect:
  - from: myresultfile.log
    to: result.log

    # ...
```

## Cleaning up the remote machine

Add all files you want to delete from the remote machine to the `clean` section. The `clean` step will be executed after the `collect` step. Files will only be cleaned if the slurm job succeeds, unless `continue_if_job_fails` is set to `true` ([see `Specifying the Slurm Batch script`](#specifying-the-slurm-batch-script)).

```yaml
clean:
  - slurm_script.sh
  - important_file_remote.txt
  - myexecutable
  - myresultfile.log
    # ...
```

## Specifying the Slurm Batch script

Specify the name of the Slurm batch script with the `job` entry.
Optionally `continue_if_job_fails` can be set to `true` to enable collecting and cleaning even if the Slurm job does not complete successfully.

```yaml
job:
  - file: slurm_script.sh
    scheduler: slurm
continue_if_job_fails: true
```


## Example configuration file

```yaml
host: $REMOTE_HOST
user: $REMOTE_USER
private_keyfile: $PRIVATE_KEY

proxyjumps:
  - host: $PROXY_HOST
    user: $PROXY_USER
    private_keyfile: $PROXY_KEY

copy:
  - from: jobs/slurm.job
    to: slurm.job
    overwrite: true

  - from: bin/myexecutable
    to: myexecutable

collect:
  - from: myresultfile.log
    to: mylocalresult.log
    overwrite: true

clean:
  - slurm.job
  - myexecutable
  - myresultfile.log

job: slurm.job
```
