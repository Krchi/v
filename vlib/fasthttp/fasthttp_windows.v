module fasthttp
f
import net
import sync.stdatomic
import time

#flag -lws2_32
#include <winsock2.h>
#include <ws2tcpip.h>

// Windows socket functions
fn C.accept(sockfd i32, addr voidptr, addrlen &u32) i32

fn C.ioctlsocket(s i32, cmd i64, argp &u32) i32

fn C.closesocket(s i32) i32

fn C.WSAGetLastError() i32

// CRT file I/O functions (prefixed with _ on Windows)
fn C._open(filename &u8, oflag i32, pmode i32) i32

fn C._close(fd i32) i32

fn C._read(fd i32, buf voidptr, max_size u32) i32

fn C._lseeki64(fd i32, offset i64, origin i32) i64

fn C._filelengthi64(fd i32) i64

// Constants
const fionbio = u64(0x8004667E) // FIONBIO: set non-blocking mode
const poll_interval_ms = 50 // sleep between non-blocking accept polls
const status_408_response = 'HTTP/1.1 408 Request Timeout\r\nContent-Type: text/plain\r\nContent-Length: 19\r\nConnection: close\r\n\r\n408 Request Timeout'.bytes()
const o_rdonly = 0 // _O_RDONLY on MSVC
const o_binary = 0x8000 // _O_BINARY on MSVC

pub struct Server {
pub:
	family                  net.AddrFamily = .ip6
	port                    int            = 3000
	max_request_buffer_size int            = 8192
	timeout_in_seconds      int            = 30
	user_data               voidptr
mut:
	socket_fd       int = -1 // listening socket
	request_handler fn (HttpRequest) !HttpResponse @[required]
	running         &stdatomic.AtomicVal[bool] = stdatomic.new_atomic(false)
	shutting_down   &stdatomic.AtomicVal[bool] = stdatomic.new_atomic(false)
	stopped         &stdatomic.AtomicVal[bool] = stdatomic.new_atomic(true)
	active_requests &stdatomic.AtomicVal[int]  = stdatomic.new_atomic(0)
}

// new_server creates and initializes a new Server instance.
pub fn new_server(config ServerConfig) !&Server {
	mut server := &Server{
		family:                  config.family
		port:                    config.port
		max_request_buffer_size: config.max_request_buffer_size
		timeout_in_seconds:      config.timeout_in_seconds
		user_data:               config.user_data
		request_handler:         config.handler
		running:                 stdatomic.new_atomic(false)
		shutting_down:           stdatomic.new_atomic(false)
		stopped:                 stdatomic.new_atomic(true)
		active_requests:         stdatomic.new_atomic(0)
	}
	return server
}

// ---- Lifecycle management methods ----
// These mirror the $if linux || bsd block in fasthttp.v so that the
// shared handle() / shutdown() / wait_till_running() logic compiles.

fn (s &Server) begin_request() {
	mut active_requests := s.active_requests
	active_requests.add(1)
}

fn (s &Server) end_request() {
	mut active_requests := s.active_requests
	active_requests.sub(1)
}

fn (s &Server) active_request_count() int {
	mut active_requests := s.active_requests
	return active_requests.load()
}

fn (s &Server) is_shutting_down() bool {
	mut shutting_down := s.shutting_down
	return shutting_down.load()
}

fn (s &Server) is_stopped() bool {
	mut stopped := s.stopped
	return stopped.load()
}

fn (mut s Server) mark_running() {
	mut running := s.running
	running.store(true)
	mut stopped := s.stopped
	stopped.store(false)
}

fn (mut s Server) mark_stopped() {
	mut active_requests := s.active_requests
	active_requests.store(0)
	mut running := s.running
	running.store(false)
	mut stopped := s.stopped
	stopped.store(true)
}

fn (mut s Server) stop_accepting() {
	if s.socket_fd >= 0 {
		C.closesocket(s.socket_fd)
		s.socket_fd = -1
	}
}

// shutdown_impl and wait_till_running_impl are needed so the shared
// ServerHandle API (fasthttp.v:186-209) can work on Windows.
fn normalized_retry_period_ms(retry_period_ms int) int {
	return if retry_period_ms > 0 { retry_period_ms } else { 1 }
}

fn (mut s Server) wait_till_running_impl(params WaitTillRunningParams) !int {
	retry_period_ms := normalized_retry_period_ms(params.retry_period_ms)
	mut attempts := 0
	mut running := s.running
	for !running.load() && attempts < params.max_retries {
		time.sleep(retry_period_ms * time.millisecond)
		attempts++
	}
	if !running.load() {
		return error('maximum retries reached')
	}
	time.sleep(retry_period_ms * time.millisecond)
	return attempts
}

fn (mut s Server) shutdown_impl(params ShutdownParams) ! {
	mut stopped := s.stopped
	if stopped.load() {
		return
	}
	mut shutting_down := s.shutting_down
	if shutting_down.compare_and_swap(false, true) {
		s.stop_accepting()
	}
	retry_period_ms := normalized_retry_period_ms(params.retry_period_ms)
	mut watch := time.new_stopwatch()
	for !stopped.load() {
		if params.timeout != time.infinite && watch.elapsed() >= params.timeout {
			return error('graceful shutdown timed out after ${params.timeout}')
		}
		time.sleep(retry_period_ms * time.millisecond)
	}
}

// ---- Socket helpers ----

fn set_nonblocking(fd int) {
	mut opt := u32(1)
	C.ioctlsocket(fd, i64(fionbio), &opt)
}

fn close_socket(fd int) {
	if fd >= 0 {
		C.closesocket(fd)
	}
}

// send_terminal_response_and_drain sends a terminal error response (e.g. 408,
// 413) and drains the receive buffer before closing the connection.  Without
// draining, Windows sends RST (WSAECONNRESET) to the client when closesocket
// is called while data is still in the receive buffer.
fn send_terminal_and_drain(fd int, response []u8) {
	C.send(fd, response.data, response.len, 0)
	// Drain up to 64KB of leftover data so the close is graceful.
	mut drain_buf := [8192]u8{}
	for _ in 0 .. 8 {
		n := C.recv(fd, &drain_buf[0], 8192, 0)
		if n <= 0 {
			break
		}
	}
}

// ---- Per-connection handler ----

struct ConnCtx {
mut:
	fd     int
	server &Server = unsafe { nil }
}

// handle_connection reads HTTP requests from the client in a persistent
// loop (HTTP/1.1 keep-alive), calling the handler for each one, sending
// the response, and optionally continuing to read the next request.
fn handle_connection(mut ctx &ConnCtx) {
	defer {
		if ctx.fd >= 0 {
			close_socket(ctx.fd)
		}
		unsafe { free(ctx) }
	}

	// Persistent connection loop for HTTP/1.1 keep-alive.
	// Each iteration reads one complete request, invokes the handler,
	// sends the response, and decides whether to continue.
	for ctx.fd >= 0 {
		// Primary buffer for headers and initial body data.
		// The fixed size (max_request_buffer_size) limits header size;
		// large bodies are read into the `body_extra` overflow buffer.
		mut buf := []u8{len: ctx.server.max_request_buffer_size, cap: ctx.server.max_request_buffer_size}
		mut read_len := 0 // bytes consumed in `buf`
		mut body_extra := []u8{} // overflow for large request bodies

		// Read the request with timeout
		mut read_start := time.sys_mono_now()
		timeout_ns := i64(ctx.server.timeout_in_seconds) * 1_000_000_000

		for {
			if read_len < ctx.server.max_request_buffer_size {
				n := C.recv(ctx.fd, unsafe { &buf[read_len] },
					ctx.server.max_request_buffer_size - read_len, 0)
				if n < 0 {
					errno_val := C.WSAGetLastError()
					if errno_val == net.error_ewouldblock {
						now := time.sys_mono_now()
						if now - read_start >= timeout_ns {
							send_terminal_and_drain(ctx.fd, status_408_response)
							return
						}
						time.sleep(poll_interval_ms * time.millisecond)
						continue
					}
					if errno_val == int(net.WsaError.wsaeconnreset) {
						// Connection reset by peer; treat as clean EOF.
						if read_len == 0 && body_extra.len == 0 {
							return
						}
						break
					}
					return
				}
				if n == 0 {
					if read_len == 0 && body_extra.len == 0 {
						return
					}
					break
				}
				read_len += int(n)
			} else {
				// Primary buffer is full; read remainder into dynamic overflow.
				mut tmp := []u8{len: ctx.server.max_request_buffer_size}
				n := C.recv(ctx.fd, tmp.data, tmp.len, 0)
				if n < 0 {
					errno_val := C.WSAGetLastError()
					if errno_val == net.error_ewouldblock {
						now := time.sys_mono_now()
						if now - read_start >= timeout_ns {
							send_terminal_and_drain(ctx.fd, status_408_response)
							return
						}
						time.sleep(poll_interval_ms * time.millisecond)
						continue
					}
					if errno_val == int(net.WsaError.wsaeconnreset) {
						break
					}
					return
				}
				if n == 0 {
					break
				}
				body_extra << tmp[..int(n)]
			}

			// Check header end
			header_end := find_header_end_in_buf(buf.data, read_len)
			if header_end > 0 {
				// Enforce header size limit
				if header_end > ctx.server.max_request_buffer_size {
					send_terminal_and_drain(ctx.fd, status_413_response)
					return
				}
				// Determine whether body is complete.
				// For Content-Length based bodies, compare received bytes
				// directly; for chunked or no-body requests, use
				// has_complete_body on combined data.
				content_length := parse_content_length_from_buf(buf.data, header_end)
				if content_length > 0 {
					body_received := (read_len - header_end) + body_extra.len
					if body_received >= content_length {
						break
					}
				} else {
					// chunked or no Content-Length; check via has_complete_body
					if body_extra.len == 0 {
						if has_complete_body(buf.data, read_len) {
							break
						}
					} else {
						total_len := read_len + body_extra.len
						mut combined := []u8{cap: total_len}
						unsafe {
							combined.push_many(buf.data, read_len)
						}
						combined << body_extra
						done := has_complete_body(combined.data, combined.len)
						unsafe { combined.free() }
						if done {
							break
						}
					}
				}
			} else if read_len >= ctx.server.max_request_buffer_size {
				// Headers still not found after filling the primary buffer;
				// exceed max header size limit.
				send_terminal_and_drain(ctx.fd, status_413_response)
				return
			}

			// Check timeout
			now := time.sys_mono_now()
			if now - read_start >= timeout_ns {
				send_terminal_and_drain(ctx.fd, status_408_response)
				return
			}

			time.sleep(10 * time.millisecond)
		}

		if read_len == 0 && body_extra.len == 0 {
			return
		}

		// Decode and handle the request
		mut request_arena := voidptr(unsafe { nil })
		$if prealloc {
			request_arena = unsafe { prealloc_scope_begin() }
		}

		mut req_buf := []u8{cap: read_len + body_extra.len}
		unsafe { req_buf.push_many(buf.data, read_len) }
		if body_extra.len > 0 {
			req_buf << body_extra
		}
		mut decoded := decode_http_request(req_buf) or {
			C.send(ctx.fd, tiny_bad_request_response.data, tiny_bad_request_response.len, 0)
			end_request_arena_current_thread(request_arena)
			return
		}

		ctx.server.begin_request()
		decoded.client_conn_fd = ctx.fd
		decoded.user_data = ctx.server.user_data

		mut resp := ctx.server.request_handler(decoded) or {
			C.send(ctx.fd, tiny_bad_request_response.data, tiny_bad_request_response.len, 0)
			end_request_arena_current_thread(request_arena)
			ctx.server.end_request()
			return
		}
		resp.attach_request_arena_if_empty(request_arena)
		// Capture should_close before the response object is consumed
		should_close_after_resp := resp.should_close

		match resp.takeover_mode {
			.manual {
				// Handler took ownership, don't close the fd
				ctx.fd = -1
				resp.free_owned_content()
				resp.abandon_request_arena_current_thread()
				ctx.server.end_request()
				return
			}
			.reusable {
				resp.free_owned_content()
				resp.end_request_arena_current_thread()
				ctx.server.end_request()
				if should_close_after_resp || ctx.server.is_shutting_down() {
					return
				}
				continue // stay open for next request
			}
			.none {}
		}

		// Build and send response
		mut content := resp.take_or_clone_content()
		mut file_fd := -1
		mut file_len := i64(0)

		if resp.file_path != '' {
			fd := C._open(resp.file_path.str, o_rdonly | o_binary, 0)
			if fd != -1 {
				file_fd = fd
				file_len = C._filelengthi64(fd)
			}
		}

		resp.request_arena = unsafe { nil }
		ctx.server.end_request()

		// Send response content
		mut sent := 0
		for sent < content.len {
			n := C.send(ctx.fd, unsafe { &content[sent] }, usize(content.len - sent), 0)
			if n < 0 {
				errno_val := C.WSAGetLastError()
				if errno_val == net.error_ewouldblock {
					time.sleep(poll_interval_ms * time.millisecond)
					continue
				}
				if file_fd != -1 {
					C._close(file_fd)
				}
				unsafe { content.free() }
				return
			}
			if n == 0 {
				if file_fd != -1 {
					C._close(file_fd)
				}
				unsafe { content.free() }
				return
			}
			sent += int(n)
		}

		// Send file body if any
		if file_fd != -1 {
			mut file_pos := i64(0)
			for file_pos < file_len {
				mut chunk := [16384]u8{}
				remaining := file_len - file_pos
				mut bytes_to_read := u32(16384)
				if i64(bytes_to_read) > remaining {
					bytes_to_read = u32(remaining)
				}
				nread := C._read(file_fd, &chunk[0], bytes_to_read)
				if nread <= 0 {
					break
				}
				s := C.send(ctx.fd, &chunk[0], usize(nread), 0)
				if s < 0 {
					errno_val := C.WSAGetLastError()
					if errno_val == net.error_ewouldblock {
						time.sleep(poll_interval_ms * time.millisecond)
						continue
					}
					break
				}
				if s == 0 {
					break
				}
				file_pos += i64(s)
				if s < nread {
					C._lseeki64(file_fd, file_pos, 0)
				}
			}
			C._close(file_fd)
		}

		unsafe { content.free() }

		// Decide whether to continue reading the next request (keep-alive)
		// or close the connection (HTTP/1.0, explicit Connection: close, or shutdown).
		if should_close_after_resp || ctx.server.is_shutting_down() {
			return
		}
		// Loop back to read the next request on the same connection
	}
}

// ---- Main entry point ----

// run starts the server, accepting connections and spawning a thread per
// connection. On Windows this uses a simple blocking accept + thread model
// since epoll/kqueue are not available.
pub fn (mut s Server) run() ! {
	// Create listening socket
	s.socket_fd = C.socket(i32(s.family), i32(net.SocketType.tcp), 0)
	if s.socket_fd < 0 {
		eprintln('ERROR: socket creation failed')
		return error('socket creation failed')
	}

	opt := 1
	C.setsockopt(s.socket_fd, C.SOL_SOCKET, C.SO_REUSEADDR, &opt, sizeof(opt))

	// On Windows IPv6 sockets default to IPV6_V6ONLY=1 (no IPv4
	// dual-stack). Enable dual-stack so that an IPv6-bound server
	// also accepts IPv4 connections (same as Linux/BSD default).
	if s.family == .ip6 {
		v6only_opt := 0
		C.setsockopt(s.socket_fd, C.IPPROTO_IPV6, C.IPV6_V6ONLY, &v6only_opt, sizeof(v6only_opt))
	}

	addr := if s.family == .ip6 {
		net.new_ip6(u16(s.port), [u8(0), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]!)
	} else {
		net.new_ip(u16(s.port), [u8(0), 0, 0, 0]!)
	}
	alen := addr.len()

	if C.bind(s.socket_fd, voidptr(&addr), alen) < 0 {
		eprintln('ERROR: bind failed on port ${s.port}')
		C.closesocket(s.socket_fd)
		s.socket_fd = -1
		return error('socket bind failed on port ${s.port}')
	}
	if C.listen(s.socket_fd, max_connection_size) < 0 {
		eprintln('ERROR: listen failed')
		C.closesocket(s.socket_fd)
		s.socket_fd = -1
		return error('socket listen failed')
	}

	// Non-blocking accept so we can check shutdown flag between accepts
	set_nonblocking(s.socket_fd)

	s.mark_running()
	println('listening on http://0.0.0.0:${s.port}/')

	for {
		if s.is_shutting_down() {
			// Wait for active requests to complete
			for s.active_request_count() > 0 {
				time.sleep(10 * time.millisecond)
			}
			break
		}

		// Non-blocking accept
		client_fd := C.accept(s.socket_fd, unsafe { nil }, unsafe { nil })
		if client_fd < 0 {
			errno_val := C.WSAGetLastError()
			if errno_val == net.error_ewouldblock {
				// No pending connections; sleep briefly to avoid busy-waiting
				time.sleep(poll_interval_ms * time.millisecond)
				continue
			}
			if s.is_shutting_down() {
				continue
			}
			// Unexpected error
			time.sleep(poll_interval_ms * time.millisecond)
			continue
		}

		// Spawn a thread to handle this connection
		mut ctx := &ConnCtx{
			fd:     client_fd
			server: s
		}
		spawn handle_connection(mut ctx)
	}

	close_socket(s.socket_fd)
	s.socket_fd = -1
	s.mark_stopped()
}