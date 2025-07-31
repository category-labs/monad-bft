// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

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
