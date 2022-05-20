# Using HPC Rocket with GitLab CI

## Add the SSH key

1. Open the file containing the SSH key for the remote machine with a text editor. Copy its content to the clipboard.

2. Go to your project in GitLab and navigate to `Settings > CI/CD > Variables`. Expand the `Variables` section and click the `Add variable` button. 

3. Paste the copied private key into the `Value` field. Enter the name `PRIVATE_KEY` into the `Key` field. Then select `File` from the `Type` dropdown menu. The final result should look similar to the image below. After entering the data, click `Add variable` to save.

    ![](_static/gitlabci/AddPrivateKey.png)

4. Add another variable with the key `REMOTE_HOST`. Copy the address of the remote machine into the `Value` field. Select `Variable` from the `Type` dropdown menu. Click `Add variable` to save.

5. Add another variable with the key `REMOTE_USER`. Enter the name of your user account on the remote machine into the `Value` field. Select `Variable` from the `Type` dropdown menu. Click `Add variable` to save.


## Add a job for HPC Rocket in your .gitlab-ci.yml

After adding the SSH keys to GitLab CI you can add a job to your `.gitlab-ci.yml` as shown below.

```yaml
run-slurm-job:
  image: python:latest
  stage: deploy

script:
  - pip install hpc-rocket
  - hpc-rocket launch --watch rocket.yml
```