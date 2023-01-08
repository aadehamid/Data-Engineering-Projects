#!/bin/bash

# Bash "strict mode", to help catch problems and bugs in the shell
# script. Every bash script you write should include this. See
# http://redsymbol.net/articles/unofficial-bash-strict-mode/ for
# details.
set -euo pipefail

# Tell apt-get we're never going to be able to give manual
# feedback:
export DEBIAN_FRONTEND=noninteractive
# Update the package listing, so we know what package exist:
apt-get update

# Install security updates:
apt-get -y upgrade
# Install a new package, without unnecessary recommended packages:
# apt-get -y install --no-install-recommends syslog-ng

# # Install Docker CE CLI
# apt-get update \
#     && apt-get install -y apt-transport-https ca-certificates curl gnupg2 lsb-release \
#     && curl -fsSL https://download.docker.com/linux/$(lsb_release -is | tr '[:upper:]' '[:lower:]')/gpg | apt-key add - 2>/dev/null \
#     && echo "deb [arch=$(dpkg --print-architecture)] https://download.docker.com/linux/$(lsb_release -is | tr '[:upper:]' '[:lower:]') $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list \
#     && apt-get update \
#     && apt-get install -y docker-ce-cli

# # Install Docker Compose
# export LATEST_COMPOSE_VERSION=$(curl -sSL "https://api.github.com/repos/docker/compose/releases/latest" | grep -o -P '(?<="tag_name": ").+(?=")') \
#     && curl -sSL "https://github.com/docker/compose/releases/download/${LATEST_COMPOSE_VERSION}/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose \
#     && chmod +x /usr/local/bin/docker-compose


export DOCKER_BUILDKIT=1

# Install Git
apt-get update \
    && apt-get install -y git





# Delete cached files we don't need anymore (note that if you're
# using official Docker images for Debian or Ubuntu, this happens
# automatically, you don't need to do it yourself):
apt-get clean
# Delete index files we don't need anymore:
rm -rf /var/lib/apt/lists/*
