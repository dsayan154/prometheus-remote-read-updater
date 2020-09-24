# prometheus-remote-read-updater

# What it does?
This is a kubernetes custom controller which watches the current replica count of a statefulset in a namespace and updates a configmap.
The watched statefulset pods are meant to be prometheus shards and the configmap to be updated is meant to contain a valid prometheus config("prometheus.yml").

## What prometheus config it updates

### This repo contains the custom controller code
Image: [dsayan154/prometheus-remote-read-updater](https://hub.docker.com/repository/docker/dsayan154/prometheus-remote-read-updater)
