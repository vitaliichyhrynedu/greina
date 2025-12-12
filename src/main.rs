use os_lab_4::hardware::storage::Storage;
use os_lab_4::kernel::Kernel;
use std::io::{self, Write};

fn main() {
    // Initialize a 64KB in-memory storage
    let storage_size = 64 * 1024;
    let storage = Storage::new(storage_size);
    let mut kernel = Kernel::new(storage);

    println!("Filesystem shell opened.");
    println!("Type 'help' for commands.");

    loop {
        // Print prompt
        print!("> ");
        io::stdout().flush().unwrap();

        // Read input
        let mut input = String::new();
        if io::stdin().read_line(&mut input).unwrap() == 0 {
            break;
        }

        // Parse command
        let parts: Vec<&str> = input.trim().split_whitespace().collect();
        if parts.is_empty() {
            continue;
        }

        let command = parts[0];
        let args = &parts[1..];

        // Execute the command as a system call
        match command {
            "mkfs" => {
                if let Some(n) = args.get(0).and_then(|s| s.parse().ok()) {
                    match kernel.mkfs(n) {
                        Ok(_) => println!("Filesystem formatted with {} nodes.", n),
                        Err(e) => println!("Error: {:?}", e),
                    }
                } else {
                    println!("Usage: mkfs <node_count>");
                }
            }
            "mount" => match kernel.mount() {
                Ok(_) => println!("Filesystem mounted."),
                Err(e) => println!("Error: {:?}", e),
            },
            "create" => {
                if let Some(name) = args.get(0) {
                    println!("{:?}", kernel.create(name));
                } else {
                    println!("Usage: create <name>");
                }
            }
            "open" => {
                if let Some(name) = args.get(0) {
                    match kernel.open(name) {
                        Ok(fd) => println!("File opened.\nfd: {}", fd),
                        Err(e) => println!("Error: {:?}", e),
                    }
                } else {
                    println!("Usage: open <name>");
                }
            }
            "close" => {
                if let Some(fd) = args.get(0).and_then(|s| s.parse().ok()) {
                    println!("{:?}", kernel.close(fd));
                } else {
                    println!("Usage: close <fd>");
                }
            }
            "read" => {
                if args.len() >= 2 {
                    let fd = args[0].parse().unwrap_or(usize::MAX);
                    let size = args[1].parse().unwrap_or(0);
                    let mut buf = vec![0u8; size];

                    match kernel.read(fd, &mut buf) {
                        Ok(bytes_read) => {
                            // Try to print as string, otherwise print bytes
                            let output = String::from_utf8_lossy(&buf[..bytes_read]);
                            println!("Read {} bytes: {:?}", bytes_read, output);
                        }
                        Err(e) => println!("Error: {:?}", e),
                    }
                } else {
                    println!("Usage: read <fd> <size>");
                }
            }
            "write" => {
                if args.len() >= 2 {
                    let fd = args[0].parse().unwrap_or(usize::MAX);
                    // Join the rest of the arguments as data
                    let data = args[1..].join(" ");
                    match kernel.write(fd, data.as_bytes()) {
                        Ok(bytes_written) => println!("Written {} bytes.", bytes_written),
                        Err(e) => println!("Error: {:?}", e),
                    }
                } else {
                    println!("Usage: write <fd> <data>");
                }
            }
            "seek" => {
                if args.len() >= 2 {
                    let fd = args[0].parse().unwrap_or(usize::MAX);
                    let offset = args[1].parse().unwrap_or(0);
                    println!("{:?}", kernel.seek(fd, offset));
                } else {
                    println!("Usage: seek <fd> <offset>");
                }
            }
            "link" => {
                if args.len() >= 2 {
                    println!("{:?}", kernel.link(args[0], args[1]));
                } else {
                    println!("Usage: link <old_name> <new_name>");
                }
            }
            "unlink" => {
                if let Some(name) = args.get(0) {
                    println!("{:?}", kernel.unlink(name));
                } else {
                    println!("Usage: unlink <name>");
                }
            }
            "truncate" => {
                if args.len() >= 2 {
                    let name = args[0];
                    let size = args[1].parse().unwrap_or(0);
                    println!("{:?}", kernel.truncate(name, size));
                } else {
                    println!("Usage: truncate <name> <size>");
                }
            }
            "stat" => {
                if let Some(name) = args.get(0) {
                    match kernel.stat(name) {
                        Ok(stats) => {
                            println!("File: {}", name);
                            println!("Type: {:?}", stats.filetype);
                            println!("Size: {}", stats.size);
                            println!("Links: {}", stats.link_count);
                            println!("Blocks: {}", stats.block_count);
                            println!("Node index: {}", stats.node_index);
                        }
                        Err(e) => println!("Error: {:?}", e),
                    }
                } else {
                    println!("Usage: stat <name>");
                }
            }
            "ls" => match kernel.ls() {
                Ok(list) => {
                    for (name, node) in list {
                        println!("{} {}", node, name);
                    }
                }
                Err(e) => println!("Error: {:?}", e),
            },
            "exit" => break,
            "help" => {
                println!("COMMANDS");
                let commands = [
                    ("mkfs <nodes>", "format filesystem"),
                    ("mount", "mount filesystem"),
                    ("create <name>", "create a file"),
                    ("open <name>", "open file"),
                    ("close <fd>", "close file"),
                    ("read <fd> <size>", "read bytes from file"),
                    ("write <fd> <string>", "write string to file"),
                    ("seek <fd> <offset>", "seek to offset"),
                    ("link <old> <new>", "create hard link"),
                    ("unlink <name>", "remove file/link"),
                    ("truncate <name> <size>", "resize file"),
                    ("stat <name>", "display file stats"),
                    ("ls", "list \"/\" directory"),
                    ("exit", "exit the shell"),
                ];
                for (cmd, desc) in commands {
                    println!("  {:<25} {}", cmd, desc);
                }
            }
            _ => println!("Unknown command: {}", command),
        }
    }
}
