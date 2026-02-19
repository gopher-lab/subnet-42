# Subnet 42 (Gopher)

This repository contains the Bittensor miner/validator "wrapper" code (neurons) for Subnet 42.

## Documentation (source of truth)

All setup, configuration, and operational docs live here:

- https://developers.gopher-ai.com/docs/subnet/intro

If something in this repository differs from the docs, treat the docs as authoritative.

## TEE worker requirement (miner + validator)

Both neurons (miner and validator) also run a TEE job worker:

- https://github.com/gopher-lab/tee-worker

Running the worker requires Intel SGX-enabled hardware (details and supported environments are described in the docs linked above).
