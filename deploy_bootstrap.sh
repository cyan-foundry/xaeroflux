#!/bin/bash
# deploy_bootstrap.sh
# Deploy updated xaeroflux_bootstrap to cyan-dev-bootstrap server
#
# Prerequisites:
# - SSH access via bastion: ssh -J cyan-dev-bastion cyan-dev-bootstrap
# - Source files in ~/xaeroflux/

set -e

BASTION="34.214.5.244"
BOOTSTRAP="10.0.32.253"
SSH_KEY="~/.ssh/cyan-infra.pem"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Deploying XaeroFlux Bootstrap (v2 - RawEvent conversion)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Step 1: Copy updated source file
echo ""
echo "📁 Step 1: Copying xaeroflux_bootstrap.rs..."

# First copy to the xaeroflux directory
cp xaeroflux_bootstrap.rs ~/xaeroflux/src/bin/xaeroflux_bootstrap.rs

# Also copy snapshot.rs
cp snapshot.rs ~/xaeroflux/src/snapshot.rs

# Add snapshot module to lib.rs if not already present
if ! grep -q "pub mod snapshot" ~/xaeroflux/src/lib.rs; then
    echo "" >> ~/xaeroflux/src/lib.rs
    echo "pub mod snapshot;" >> ~/xaeroflux/src/lib.rs
    echo "Added snapshot module to lib.rs"
fi

# Step 2: Build for Linux
echo ""
echo "🔨 Step 2: Building for Linux (x86_64-unknown-linux-gnu)..."

cd ~/xaeroflux

# Cross-compile using Docker
docker run --rm --platform linux/amd64 \
    -v "$(pwd)":/app -w /app \
    rust:alpine \
    sh -c "apk add musl-dev openssl-dev openssl-libs-static pkgconfig perl make && \
           rustup target add x86_64-unknown-linux-musl && \
           OPENSSL_STATIC=1 cargo build --release --bin xaeroflux_bootstrap --target x86_64-unknown-linux-musl"

# Step 3: Deploy to bootstrap server
echo ""
echo "🚀 Step 3: Deploying to bootstrap server..."

# Copy binary via bastion
scp -i $SSH_KEY -o ProxyJump=ubuntu@$BASTION \
    target/x86_64-unknown-linux-musl/release/xaeroflux_bootstrap \
    ubuntu@$BOOTSTRAP:/tmp/xaeroflux_bootstrap

# Stop, replace, start
ssh -i $SSH_KEY -o ProxyJump=ubuntu@$BASTION ubuntu@$BOOTSTRAP << 'EOF'
    sudo systemctl stop xaeroflux-bootstrap || true
    sudo mv /tmp/xaeroflux_bootstrap /opt/cyan/bin/xaeroflux_bootstrap
    sudo chmod +x /opt/cyan/bin/xaeroflux_bootstrap
    sudo systemctl start xaeroflux-bootstrap
    sleep 2
    sudo systemctl status xaeroflux-bootstrap --no-pager
EOF

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  ✅ Bootstrap deployed!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Check logs: ssh -J cyan-dev-bastion cyan-dev-bootstrap 'journalctl -u xaeroflux-bootstrap -f'"
