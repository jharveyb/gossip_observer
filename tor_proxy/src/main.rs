use tor_proxy::spawn_forwarder;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let forwarder_1 = spawn_forwarder(
        // OS will assign us an unused port in the ephemeral range / above 32768
        "127.0.0.1:0",
        // Tor daemon local listening addr; SOCKS proxy is enabled by default.
        "127.0.0.1:9050",
        // Tor Project debian pkg repo onion service
        // https://support.torproject.org/little-t-tor/getting-started/apt-over-tor/
        "apow7mjfryruh65chtdydfmqfpj5btws7nbocgtaovhvezgccyjazpqd.onion",
        80,
    );

    // Keep the main thread alive
    Ok(tokio::try_join!(forwarder_1)?.0)
}
