use async_std::{
    io::{stdin, stdout, BufReader, prelude::BufReadExt},
    net::{TcpStream, ToSocketAddrs},
    prelude::*,
    task,
};
use log::debug;
use async_std::net::SocketAddr;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

struct Connection {
    br: BufReader<TcpStream>,
}
impl Connection {
    async fn open(addr: impl ToSocketAddrs) -> Result<Self> {
        let s = TcpStream::connect(addr).await?;
        let br = BufReader::new(s);
        Ok(Self { br })
    }

    async fn addr(&self) -> Result<SocketAddr> {
        let addr = self.br.get_ref().peer_addr()?;
        Ok(addr)
    }

    async fn reopen(&mut self) -> Result<()> {
        debug!("reconnecting");
        let addr = self.addr().await?;
        let s = TcpStream::connect(addr).await?;
        let br = BufReader::new(s);
        self.br = br;
        debug!("reconnected");
        Ok(())
    }

    async fn read_line(&mut self, buf: &mut String) -> Result<()> {
        self.br.read_line(buf).await?;
        debug!("read: {:?}", buf);
        Ok(())
    }

    async fn write(&mut self, s: &str) -> Result<()> {
        // TODO: remove
        let s = s.replace(".", "\r\n");
        self.br.get_mut().write_all(s.as_bytes()).await?;
        self.br.get_mut().write_all(b"\r\n").await?;
        debug!("sent: {:?}", s);
        Ok(())
    }
}

async fn eval(conn: &mut Connection, cmd: &str) -> Result<String> {
    conn.write(cmd).await?;

    let mut lines = vec![];

    let mut remaining = 1;
    let mut is_meta = true;
    loop {
        if remaining == 0 { break; }
        remaining -= 1;

        let mut resp = String::new();
        conn.read_line(&mut resp).await?;
        if resp.starts_with("+") || resp.starts_with("-") || resp.starts_with(":") {
            is_meta = false;
        }
        if is_meta {
            if resp.starts_with("$-1") || resp.starts_with("*-1") {
                resp.clear();
                resp.push_str("null");
            } else if resp.starts_with("$") {
                remaining += 1;
            } else if resp.starts_with("*") {
                let count = resp.trim_start_matches("*").trim_end_matches("\r\n");
                let count = count.parse::<u32>()?;
                remaining += count;
                is_meta = false;
            }
        } else {
            lines.push(resp.trim_end_matches("\r\n").to_string());
        }
        if resp.starts_with("-") {
            debug!("error: {}", resp);
            conn.reopen().await?;
        }
        is_meta = !is_meta;
    }
    debug!("lines: {:?}", lines);
    Ok(lines.join(" "))
}

async fn prompt(p: &str) -> Result<String> {
    let mut out = stdout();
    out.write_all(p.as_bytes()).await?;
    out.flush().await?;
    let mut s = String::new();
    stdin().read_line(&mut s).await?;
    Ok(s)
}

async fn run(addr: impl ToSocketAddrs) -> Result<()> {
    let mut conn = Connection::open(addr).await?;
    loop {
        let input = prompt(&format!("redis({})> ", conn.addr().await?)).await?;
        if input.trim().is_empty() { continue; }
        let res = eval(&mut conn, &input).await?;
        println!("result: {}", res);
    }
}

pub fn main() {
    pretty_env_logger::init();
    let r = task::block_on(run("127.0.0.1:6379"));

    if let Err(e) = r {
        eprintln!("error: {}", e);
    };
}
