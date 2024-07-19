use crate::Instance;
use tokio::runtime::Runtime;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;


pub fn launch_server(instance: Instance) -> anyhow::Result<()> {
    let rt = Runtime::new()?;

    rt.block_on(async {
        let listener = TcpListener::bind("0.0.0.0:8080").await?;
        loop {
            let (mut socket, _) = listener.accept().await?;
            tokio::spawn(async move {
                let mut queue = String::new();
                loop {
                    let mut temp = String::new();
                    socket.read_to_string(&mut temp);
                    queue.extend(temp);
                    let commands = queue.split("\n").collect::<Vec<&str>>(); 
                    if !commands.is_empty() {
                        let len = if queue.ends_with("\n") {
                            commands.len() 
                        } else {
                            commands.len() - 1; 
                        }
                        for command in commands.iter().take(len) {
                            //instance.execute(command);
                        }
                        // instance.execute(
                    
                    }
                }
                
            });
        }
    })
}
