# 🚀 Subnet-42 Miner with VPN Setup Guide

This guide helps you set up your Subnet-42 miner with a VPN for residential IP routing. This allows your miner to be publicly accessible while your worker routes outbound traffic through a residential VPN IP.

## 📋 Prerequisites

- Docker installed
- **TorGuard VPN subscription** (strongly recommended for residential IPs)
- Twitter account credentials

## 🔧 Setup Steps

### 1️⃣ Prepare Your VPN Configuration

#### TorGuard Setup for Residential IPs

TorGuard is specifically recommended because they offer residential IP addresses, which are crucial for this setup.

1. **Subscribe to TorGuard with Residential IP add-on**:

   - Sign up for TorGuard VPN
   - Purchase the "Residential IP" add-on
   - Request residential IPs in your desired location

2. Create the required directories:

   ```bash
   mkdir -p vpn cookies
   ```

3. **Create auth.txt file**:

   Create a file with your TorGuard credentials:

   ```
   your_torguard_username
   your_torguard_password
   ```

   Save this to `vpn/auth.txt`

4. **Configure OpenVPN**:

   - Log into your TorGuard account
   - Download the OpenVPN configuration files
   - Create a `config.ovpn` file in `vpn/` with your residential servers:

   ```
   client
   dev tun
   proto udp
   # Multiple residential servers for redundancy
   remote <ip-here> <port>
   remote <ip-here> <port>
   remote-random
   remote-cert-tls server
   auth SHA256
   key-direction 1
   # Add your TorGuard certificates and keys below
   ... (rest of configuration) ...
   ```

### 2️⃣ Generate Twitter Cookies

#### Option 1: Use the Cookie Generator Service

1. **Configure Twitter Credentials**:

   Add your Twitter account credentials to your .env file:

   ```
   # Add your Twitter accounts in this format
   TWITTER_ACCOUNTS="username1:password1,username2:password2"
   ```

2. **Run the Cookie Generator Service**:

   ```bash
   docker compose up cookies
   ```

   This service will:

   - Log in to your Twitter accounts
   - Generate authentication cookies
   - Save them to the `cookies/` directory in your project

3. **Verify Cookie Generation**:

   ```bash
   ls -la ./cookies/
   ```

   You should see files named `<username>_twitter_cookies.json` for each account.

### 3️⃣ Launch Everything with One Command

Once you have:

- VPN files in `vpn/` (auth.txt and config.ovpn)
- Cookie files in the `cookies/` directory

You can start the full system:

```bash
docker compose --profile miner-vpn up -d
```

This command will:

1. Start the VPN service using your TorGuard residential IPs
2. Launch the TEE worker using the VPN
3. Start your subnet-42 miner

## 🧪 Testing Your Setup

### Verify Worker-VPN Routing

To make sure your worker-vpn container is properly routing through the VPN:

1. **Install curl in the worker-vpn container**:

   ```bash
   docker exec -it worker-vpn apt-get update && docker exec -it worker-vpn apt-get install -y curl
   ```

2. **Check the IP address of the worker-vpn container**:

   ```bash
   # Get the worker-vpn IP
   docker exec worker-vpn curl -s https://ifconfig.me
   ```

   Verify that this shows a different IP than your host machine, confirming traffic is routing through the VPN.

### Why Residential IPs Matter

Regular datacenter VPN IPs are often flagged and blocked by services. Residential IPs are much less likely to be detected, making them essential for reliable operation.
