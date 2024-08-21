use crate::types::DechibMessage;
use dechib_core::Instance;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::runtime::Runtime;

const MAX_BUFFER: usize = u16::MAX as usize;

pub fn launch_server(_instance: Instance) -> anyhow::Result<()> {
    let rt = Runtime::new()?;

    rt.block_on(async {
        let listener = TcpListener::bind("0.0.0.0:8080").await?;
        loop {
            let (mut socket, _) = listener.accept().await?;
            tokio::spawn(async move {
                let mut buf = [0u8; MAX_BUFFER];
                loop {
                    let n = match socket.read(&mut buf).await {
                        Ok(n) if n == 0 => break,
                        Ok(n) => n,
                        Err(e) => {
                            eprintln!("Failed to read from socket; err = {:?}", e);
                            break;
                        }
                    };

                    let message =
                        DechibMessage::try_from(&buf[..n]).unwrap_or_else(|err| panic!("{}", err));

                    if let Err(error) = socket.write_all(&message.message_content[..]).await {
                        eprintln!("failed to write to socket: {:?}", error);
                        break;
                    }
                }
            });
        }
    })
}
