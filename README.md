# Rabbit Cleaner

Utility that cleans up unused queues from RabbitMQ. It searches for and deletes 
queues that match a regex pattern that have been idle for the configured 
period at configured interval.

## Installation

### With Docker

This program is also available as a lightweight Docker container from Docker 
Hub [cyrusbio/rabbit-cleaner](https://hub.docker.com/r/cyrusbio/rabbit-cleaner)

### From Source

This program should work with either Python 2 or 3. Pull this repository, and 
install the Python modules listed in `requirements.txt`

## Usage

Pass `--help` flag to the program and it will display options and their 
defaults.
