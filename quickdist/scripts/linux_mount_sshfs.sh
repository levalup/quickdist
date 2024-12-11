#!/usr/bin/env bash

set -eo pipefail

function mount_sshfs {
    if [ "$#" -lt 5 ] || [ "$#" -gt 6 ]; then
        echo "Usage: mount_sshfs <local_path> <remote_path> <remote_ip> <remote_port> <remote_user> [<remote_password>]"
        return 1
    fi

    local local_path=$1
    local remote_path=$2
    local remote_ip=$3
    local remote_port=$4
    local remote_user=$5
    local remote_password=$6

    if ! command -v sshfs > /dev/null 2>&1; then
        echo "Error: sshfs command not found. Please install it."
        return 1
    fi

    if [ -n "$remote_password" ]; then
        if ! command -v sshpass > /dev/null 2>&1; then
            echo "Error: sshpass command not found. Please install it."
            return 1
        fi
    fi

    local sshfs_command="\
        sshfs \
        -o StrictHostKeyChecking=no \
        -o ServerAliveInterval=15,ServerAliveCountMax=3 \
        -o port=$remote_port \
        $remote_user@$remote_ip:$remote_path $local_path \
        "

    if [ -n "$remote_password" ]; then
        sshfs_command="sshpass -p '$remote_password' $sshfs_command"
    fi

    echo "Mounting remote directory..."
    if eval "$sshfs_command"; then
        echo "Successfully mounted remote directory from $remote_ip:$remote_path to $local_path"
    else
        echo "Failed to mount remote directory."
        return 1
    fi
}

function manage_mount_path {
    local mount_path="$1"

    if [ -z "$mount_path" ]; then
        echo "Error: No path provided."
        return 1
    fi

    if [ ! -d "$mount_path" ]; then
        echo "Creating directory: $mount_path"
        if ! mkdir -p "$mount_path"
        then
            echo "Failed to create directory: $mount_path"
            return 1
        fi
    fi

    if mountpoint -q "$mount_path"; then
        echo "Unmounting directory: $mount_path"
        if ! fusermount -u "$mount_path"
        then
            echo "Failed to unmount directory: $mount_path"
            return 1
        fi
    fi

    if [ "$(ls -A "$mount_path")" ]; then
        echo "Directory is not empty: $mount_path"
        return 1
    else
        # echo "Directory exists, is empty, and is not mounted: $mount_path"
        return 0
    fi
}

export -f mount_sshfs
export -f manage_mount_path
