echo "🛠️ Building for linux / amazon linux ec2 "
docker run --rm --platform linux/amd64 -v "$PWD":/app -w /app rust:latest cargo build --release --bin xaeroflux_bootstrap
echo "↗️ uploading file to tmp"
scp target/release/xaeroflux_bootstrap cyan-dev-bootstrap:/tmp/
echo "🔋restarting system"
ssh cyan-dev-bootstrap "sudo mv /tmp/xaeroflux_bootstrap /opt/cyan/bin/ && sudo systemctl restart xaeroflux-bootstrap"