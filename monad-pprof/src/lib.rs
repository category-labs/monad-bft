use actix_server::Server;
use actix_web::{App, HttpServer, web};

mod heap;

pub fn start_pprof_server(addr: String) -> std::io::Result<Server> {
    Ok(HttpServer::new(|| {
        App::new()
            .route("/debug/pprof/heap", web::get().to(heap::handle_get_heap))
            .route(
                "/debug/pprof/heap/config",
                web::post().to(heap::handle_update_prof_config),
            )
    })
    .bind(addr)?
    .workers(1)
    .run())
}
