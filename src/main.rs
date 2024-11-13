use async_trait::async_trait;
use pingora::prelude::*;

use pingora_core::services::background::background_service;
use std::{sync::Arc, time::Duration};

use pingora_load_balancing::{selection::RoundRobin, LoadBalancer};
use pingora_core::upstreams::peer::HttpPeer;
use pingora_core::Result;
use pingora_proxy::{ProxyHttp, Session};
use pingora::http::ResponseHeader;

use once_cell::sync::Lazy;
use pingora_limits::rate::Rate;

pub struct LB(Arc<LoadBalancer<RoundRobin>>);

impl LB {
    pub fn get_request_appid(&self, session: &mut Session) -> Option<String> {
        match session
            .req_header()
            .headers
            .get("appid")
            .map(|v| v.to_str())

        {
            None => None,
            Some(v) => match v {
                Ok(v) => Some(v.to_string()),
                Err(_) => None,
            },
        }    
    }
}

static RATE_LIMITER: Lazy<Rate> = Lazy::new(|| Rate::new(Duration::from_secs(1)));

static MAX_REQ_PER_SEC: isize = 1;


#[async_trait]
impl ProxyHttp for LB {
    type CTX = ();
    fn new_ctx(&self) -> () {
        ()
    }

    async fn upstream_peer(&self, _session: &mut Session, _ctx: &mut ()) -> Result<Box<HttpPeer>> {
       let upstream = self.0
            .select(b"", 256)
            .unwrap();
        
        println!("upstream peer is: {upstream:?}");
        
        let peer = Box::new(HttpPeer::new(upstream, true, "one.one.one.one".to_string()));
        Ok(peer)
     
    }

    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        upstream_request: &mut RequestHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()> {
        upstream_request.insert_header("Host", "one.one.one.one").unwrap();
        Ok(())
    }

    async fn request_filter(&self, 
        session: &mut Session,
        _ctx: &mut Self::CTX) -> Result<bool>
        where
            Self::CTX: Send + Sync,
        {
            let appid = match self.get_request_appid(session) {
                None => return Ok(false),
                Some(addr) => addr,
            };

            let curr_window_request = RATE_LIMITER.observe(&appid, 1);
            if curr_window_request > MAX_REQ_PER_SEC {
                let mut header = ResponseHeader::build(429, None).unwrap();
                header
                    .insert_header("X-Rate-Limit-Limit", MAX_REQ_PER_SEC.to_string())
                    .unwrap();
                header.insert_header("X-Rate-Limit-Remaining", "0").unwrap();
                header.insert_header("X-Rate-Limit-Reset", "1").unwrap();
                session.set_keepalive(None);
                session
                    .write_response_header(Box::new(header), true)
                    .await?;
                return Ok(true);        
            }
            Ok(false)

        }


}

fn main() {
    let mut my_server = Server::new(None).unwrap();
    my_server.bootstrap();

    let mut upstreams =
        LoadBalancer::try_from_iter(["1.1.1.1:443", "1.0.0.1:443", 
        "127.0.0.1:343"]).unwrap();  

    let hc = TcpHealthCheck::new();
    upstreams.set_health_check(hc);
    upstreams.health_check_frequency = Some(Duration::from_secs(1));

    let background = background_service("health check", upstreams);
    let upstreams = background.task();

    let mut lb = http_proxy_service(&my_server.configuration, LB(upstreams));
    lb.add_tcp("0.0.0.0:6188");

    my_server.add_service(background);
    
    my_server.add_service(lb);
    my_server.run_forever();
}
