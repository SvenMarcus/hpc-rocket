# Generating SSH keys to log into a remote machine

1. Generate a new SSH key pair using `ssh-keygen`. When prompted, leave the pass phrase empty and just confirm with enter. **The private SSH key serves as login information to the remote machine. Do not share it with anybody!**
    ```bash
    ssh-keygen -t ed25519 -f my_ssh_key
    ```

2. Copy the generated SSH key to the cluster with `ssh-copy-id`.
    ```bash
    ssh-copy-id -i my_ssh_key myuser@cluster.example.com
    ```