#!/bin/bash
set -euo pipefail

DEFAULT_REGION="nyc2"
DEFAULT_SIZE="s-1vcpu-2gb"
DEFAULT_IMAGE="debian-13-x64"

# TODO: update to add an optional volume
# Usage function
usage() {
    cat << EOF
Usage: $0 -n HOSTNAME [-r REGION] [-s SIZE]

Required:
    -n HOSTNAME     Hostname for the droplet

Optional:
    -r REGION       DigitalOcean region (default: $DEFAULT_REGION)
    -s SIZE         Droplet size (default: $DEFAULT_SIZE)
    -h              Show this help message

Examples:
    $0 -n nats-prod -k DO_KEY_NAME
    $0 -n collector-01 -r sfo3 -s s-2vcpu-2gb -k DO_KEY_NAME

Available regions: nyc1, nyc3, sfo3, ams3, sgp1, lon1, fra1, tor1, blr1
Common sizes: s-1vcpu-1gb, s-1vcpu-2gb, s-2vcpu-2gb, s-2vcpu-4gb
EOF
    exit 1
}

# Parse arguments
HOSTNAME=""
SSH_KEY_NAME=""
REGION="$DEFAULT_REGION"
SIZE="$DEFAULT_SIZE"

while getopts "n:r:s:h:k" opt; do
    case $opt in
        n) HOSTNAME="$OPTARG" ;;
        r) REGION="$OPTARG" ;;
        s) SIZE="$OPTARG" ;;
        k) SSH_KEY_NAME="$OPTARG" ;;
        h) usage ;;
        *) usage ;;
    esac
done

# Validate required arguments
if [[ -z "$HOSTNAME" ]]; then
    echo "Error: Hostname is required"
    usage
fi

if [[ -z "$SSH_KEY_NAME" ]]; then
    echo "Error: SSH key name is required"
    usage
fi

echo "==> Provisioning droplet: $HOSTNAME"
echo "    Region: $REGION"
echo "    Size: $SIZE"
echo ""

# Get SSH key ID
echo "==> Finding SSH key: $SSH_KEY_NAME"
SSH_KEY_ID=$(doctl compute ssh-key list --format ID,Name --no-header | \
    grep "$SSH_KEY_NAME" | awk '{print $1}')

if [[ -z "$SSH_KEY_ID" ]]; then
    echo "Error: SSH key '$SSH_KEY_NAME' not found"
    echo "Available keys:"
    doctl compute ssh-key list
    exit 1
fi
echo "    Found SSH key ID: $SSH_KEY_ID"

# Get vpc UUID
echo "==> Finding vpc UUID for region"
VPC_ID=$(doctl vpcs list --format ID,Region --no-header | \
    grep "$REGION" | awk '{print $1}')

if [[ -z "$VPC_ID" ]]; then
    echo "Error: VPC $VPC_ID not found"
    echo "Available VPCs:"
    doctl vpcs list
    exit 1
fi
echo "    Found VPC ID: $VPC_ID"

# Create droplet
echo "==> Creating droplet..."
DROPLET_OUTPUT=$(doctl compute droplet create "$HOSTNAME" \
    --region "$REGION" \
    --vpc-uuid "$VPC_ID" \
    --size "$SIZE" \
    --image "$DEFAULT_IMAGE" \
    --ssh-keys "$SSH_KEY_ID" \
    --tag-name "gobserver" \
    --enable-ipv6 \
    --enable-monitoring \
    --wait \
    --format ID,Name,PublicIPv4,Status \
    --no-header)

DROPLET_ID=$(echo "$DROPLET_OUTPUT" | awk '{print $1}')
DROPLET_IP=$(echo "$DROPLET_OUTPUT" | awk '{print $3}')

if [[ -z "$DROPLET_ID" ]]; then
    echo "Error: Failed to create droplet"
    exit 1
fi

echo "    Created droplet ID: $DROPLET_ID"
echo "    Public IP: $DROPLET_IP"

echo ""
echo "==> Droplet provisioned successfully!"
echo "    ID: $DROPLET_ID"
echo "    Name: $HOSTNAME"
echo "    IP: $DROPLET_IP"
echo "    Region: $REGION"
echo "    Size: $SIZE"
echo ""
echo "Next steps:"
echo "  1. Wait ~30 seconds for SSH to be ready"
echo "  2. Add to Ansible inventory:"
echo "     $HOSTNAME ansible_host=$DROPLET_IP"
echo "  3. Run Ansible playbooks"
