# SUBNET 42

A Bittensor subnet for MASA's Subnet 42.

## Prerequisites

1. A registered hotkey on subnet 165 on the test network, or 42 on finney (mainnet)
2. Docker and Docker Compose installed
3. Your coldkey and hotkey mnemonics

## Quick Start

1. Clone and configure:

```bash
git clone https://github.com/masa-finance/subnet-42.git
cd subnet-42
cp .env.example .env
```

2. Edit `.env` with your keys:

```env
# Your coldkey mnemonic
COLDKEY_MNEMONIC="your coldkey mnemonic here"

# Your hotkey mnemonic (must be already registered on subnet 165)
HOTKEY_MNEMONIC="your hotkey mnemonic here"
```

3. Run as a validator or miner:

```bash
# Run as a validator
docker compose --profile validator up

# Run as a miner
docker compose --profile miner up
```

The containers will automatically pull the latest images from Docker Hub.

## Configuration

Required environment variables in `.env`:

```env
COLDKEY_MNEMONIC           # Your coldkey mnemonic
HOTKEY_MNEMONIC           # Your hotkey mnemonic (must be registered on subnet 165)
ROLE                      # Either "validator" or "miner"
```

Optional environment variables in `.env`:

```env
NETUID=165                # Subnet ID (default: 165)
SUBTENSOR_NETWORK=test    # Network (default: test)
VALIDATOR_PORT=8092       # Port for validator API (default: 8092)
MINER_PORT=8091          # Port for miner API (default: 8091)
```

## Monitoring

View logs:

```bash
# All logs
docker compose logs -f

# Specific service logs
docker compose logs subnet42 -f      # Main service
docker compose logs tee-worker -f    # TEE worker (miner only)
```

## Verification

To verify your node is running correctly:

1. Check if your hotkey is registered:

```bash
btcli s metagraph --netuid 165 --network test
```

2. Check the logs:

```bash
docker compose logs subnet42 -f
```

You should see:

- Successful connection to the test network
- Your hotkey being loaded
- For validators: Connection attempts to miners (note: on testnet, many miners may be offline which is normal)
- For miners: TEE worker initialization and connection to validators

## Cookie Management

This project supports cookie refresh systems for updating Twitter authentication cookies across different deployment environments.

### Generate Cookie Files Only

To generate fresh Twitter cookie files without uploading them:

```bash
# Generate cookies only (creates JSON files in ./cookies/)
docker compose up cookies-generator
```

### Docker Cookie Refresh (for Docker-based miners)

Use this when your miners are running in Docker containers:

```bash
# Generate cookies and update Docker-based miners
docker compose up cookies-updater-docker
```

This will:

1. Generate fresh Twitter cookies using the `cookies-generator` service (automatic dependency)
2. Update all Docker containers with `cookies-volume` volumes using SSH

**Requirements:**

- `key.pem` file in the project root for SSH access to remote Docker hosts
- Environment variables in `.env`:
  ```env
  COOKIES_REMOTE_HOSTS=host1,host2,host3  # Comma-separated list of Docker hosts
  COOKIES_REMOTE_USER=azureuser           # SSH username
  COOKIES_REMOTE_DIR=/tmp/cookies-upload  # Temporary directory on remote hosts
  ```

### Kubernetes Cookie Refresh (for Kubernetes-based miners)

Use this when your miners are running in Kubernetes:

```bash
# Generate cookies and update Kubernetes-based miners
docker compose up cookies-updater-kubernetes
```

This will:

1. Generate fresh Twitter cookies using the `cookies-generator` service (automatic dependency)
2. Update all Kubernetes pods using `kubectl cp`

**Requirements:**

- `kubeconfig.yaml` kubeconfig file in the project root
- Environment variables in `.env`:
  ```env
  DEPLOYMENTS=deployment1,deployment2,deployment3  # Comma-separated list of deployments
  NAMESPACE=your-namespace                         # Kubernetes namespace
  # Optional: USE_TAR_BULK_COPY=true (default) uploads one tarball per pod for much faster uploads
  ```

### Upload Existing Cookies Only

If you already have cookie files and want to upload them without regenerating:

```bash
# Upload existing cookies to Docker-based miners (skip generation)
docker compose up cookies-updater-docker --no-deps

# Upload existing cookies to Kubernetes-based miners (skip generation)
docker compose up cookies-updater-kubernetes --no-deps
```

The `--no-deps` flag skips the `cookies-generator` dependency and uploads whatever cookie files are already present in the `./cookies/` directory.

### Twitter Account Configuration

For all cookie operations, configure your Twitter accounts in `.env`:

```env
TWITTER_ACCOUNTS="username1:password1,username2:password2"
TWITTER_EMAIL="your_email@example.com"  # Required for verification
```

## Troubleshooting

1. Pull latest images:

```bash
docker compose pull
```

2. Clean start:

```bash
# Stop and remove everything
docker compose down -v

# Start fresh as validator
docker compose --profile validator up

# Or start fresh as miner
docker compose --profile miner up
```

3. Common issues:

- Ensure your hotkey is registered on subnet 165 (glagolitic_yu) on the test network
- Check logs for any initialization errors
- Verify your mnemonics are correct
- For validators: Connection errors to miners on testnet are normal as many may be offline
- For miners: Ensure TEE worker is running and accessible

4. Cookie refresh issues:

- For Docker: Ensure `key.pem` SSH key has correct permissions and access to remote hosts
- For Kubernetes: Ensure `k8s.yml` kubeconfig is valid and has access to the specified namespace
- Check that `TWITTER_ACCOUNTS` and `TWITTER_EMAIL` are properly configured in `.env`
