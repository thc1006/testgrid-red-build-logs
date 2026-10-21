"""The pooled HTTP layer: keep-alive reuse, never reusing a connection with unread
data, per-origin request limits, and redirects."""
import http.client
import http.server
import io
import os
import sys
import threading
import unittest
import urllib.error
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_red_logs as frl  # noqa: E402
from test_fetch_red_logs import FakeServer, Handler  # noqa: E402


class KeepAliveHandler(Handler):
    protocol_version = "HTTP/1.1"
    timeout = 5  # an idle kept-alive connection ends its handler thread after this

    def handle_one_request(self):
        self.head = False  # one handler serves every request on a kept-alive connection
        super().handle_one_request()


class CountingServer(http.server.ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False  # do not wait for kept-alive connections on close

    def get_request(self):
        sock, addr = super().get_request()
        with self.lock:
            self.connections += 1
        return sock, addr

    def handle_error(self, request, client_address):
        # a client closing a kept-alive connection mid-response is what some tests do on purpose
        if not isinstance(sys.exc_info()[1], ConnectionError):
            super().handle_error(request, client_address)


class KeepAliveServer(FakeServer):
    """FakeServer speaking HTTP/1.1 (connections stay open) that counts the connections it accepts."""

    def __init__(self):  # (not FakeServer.__init__, which starts an HTTP/1.0 server)
        self.httpd = CountingServer(("127.0.0.1", 0), KeepAliveHandler)
        self.httpd.lock, self.httpd.connections = threading.Lock(), 0
        self.httpd.routes, self.httpd.hits, self.httpd.prefix_routes = {}, {}, {}
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}"
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    @property
    def connections(self):
        return self.httpd.connections


class PoolWorld(unittest.TestCase):
    def setUp(self):
        frl.BACKOFF = 0
        self.ctx = frl.RunContext(frl.AdaptiveLimit(4, 4))
        frl.bind_context(self.ctx)
        self.srv = KeepAliveServer()
        self.addCleanup(self.srv.close)
        self.addCleanup(frl.bind_context, frl._DEFAULT_CONTEXT)


class KeepAliveTest(PoolWorld):
    def test_requests_to_one_origin_share_a_connection(self):
        self.srv.route("/a", (200, {}, b"first"))
        self.srv.route("/b", (200, {}, b"second"))
        self.assertEqual(frl.request(self.srv.url + "/a"), b"first")
        self.assertEqual(frl.request(self.srv.url + "/b"), b"second")
        self.assertEqual(frl.request(self.srv.url + "/a", consume=lambda r: r.headers.get("Content-Length"),
                                     method="HEAD"), "5")
        self.assertEqual(frl.request(self.srv.url + "/b"), b"second")
        self.assertEqual(self.srv.connections, 1)

    def test_a_connection_with_unread_data_is_never_reused(self):
        big = b"x" * (1 << 20)
        self.srv.route("/big", (200, {}, big))
        self.srv.route("/small", (200, {}, b"small"))

        def read_a_little(r):
            r.read1(10)
            raise RuntimeError("stop reading here")
        with self.assertRaises(RuntimeError):
            frl.request(self.srv.url + "/big", consume=read_a_little)
        self.assertEqual(frl.request(self.srv.url + "/small"), b"small")  # not the rest of /big
        self.assertEqual(self.srv.connections, 2)

    def test_an_error_response_closes_its_connection(self):
        # its body is not read (a server could trickle it without a deadline): the next request opens another
        self.srv.route("/gone", (403, {}, b"<Error>denied</Error>"))
        self.srv.route("/ok", (200, {}, b"fine"))
        with self.assertRaises(urllib.error.HTTPError):
            frl.request(self.srv.url + "/gone")
        self.assertEqual(frl.request(self.srv.url + "/ok"), b"fine")
        self.assertEqual(frl.request(self.srv.url + "/ok"), b"fine")
        self.assertEqual(self.srv.connections, 2)


class OriginLimitTest(PoolWorld):
    def test_one_origin_pushing_back_does_not_slow_the_other(self):
        other = KeepAliveServer()
        self.addCleanup(other.close)
        self.srv.route("/busy", (503, {}, b""))
        other.route("/ok", (200, {}, b"fine"))
        with self.assertRaises(urllib.error.HTTPError):
            frl.request(self.srv.url + "/busy")
        self.assertEqual(frl.request(other.url + "/ok"), b"fine")
        stats = self.ctx.stats()["origins"]
        self.assertEqual(stats[self.srv.url]["final"], 2)  # one burst of pushbacks halves it once
        self.assertEqual(stats[self.srv.url]["pushbacks"], frl.RETRIES)
        self.assertEqual((stats[other.url]["final"], stats[other.url]["pushbacks"]), (4, 0))
        self.assertEqual((self.ctx.stats()["final"], self.ctx.stats()["pushbacks"]), (2, frl.RETRIES))

    def test_a_body_cut_short_is_retried_without_counting_as_pushback(self):
        def cut(h):
            h.send_response(200)
            h.send_header("Content-Length", "100")
            h.send_header("Connection", "close")
            h.end_headers()
            h.wfile.write(b"0123456789")
        self.srv.route("/cut", cut)
        with self.assertRaises(frl.IntegrityError):
            frl.request(self.srv.url + "/cut")
        self.assertEqual(self.ctx.stats()["pushbacks"], 0)


class RedirectTest(PoolWorld):
    def test_same_host_redirects_are_followed_and_other_hosts_refused(self):
        other = KeepAliveServer()
        self.addCleanup(other.close)
        self.srv.route("/old", (302, {"Location": "/new"}, b""))
        self.srv.route("/new", (200, {}, b"moved"))
        self.srv.route("/away", (301, {"Location": other.url + "/x"}, b""))
        self.srv.route("/loop", (307, {"Location": "/loop"}, b""))
        self.assertEqual(frl.request(self.srv.url + "/old"), b"moved")
        self.assertEqual(frl.request(self.srv.url + "/old", consume=lambda r: r.url, method="HEAD"), self.srv.url + "/new")
        with self.assertRaisesRegex(urllib.error.HTTPError, "refused redirect to another host"):
            frl.request(self.srv.url + "/away")
        with self.assertRaisesRegex(urllib.error.HTTPError, "redirects"):
            frl.request(self.srv.url + "/loop")
        self.assertEqual(self.srv.hits("/loop"), frl.MAX_REDIRECTS + 1)
        self.assertEqual(other.hits("/x"), 0)


UNPARSABLE = "X-Unparsable"  # (a header the parser of unparsable() fails on)


def unparsable(real=http.client.parse_headers, error=None):
    """http.client's header parser, failing on a response with UNPARSABLE (with error, or as CPython 3.12's fails
    on some Content-Types: python/cpython#134151), whatever the Python that runs the tests."""
    def parse(fp, *args, **kwargs):
        headers = real(fp, *args, **kwargs)
        if headers.get(UNPARSABLE):
            raise error or TypeError("'<' not supported between instances of 'int' and 'NoneType'")
        return headers
    return parse


class UnparsableResponseTest(PoolWorld):
    def test_one_http_client_cannot_parse_is_one_that_cannot_be_read(self):
        # (retried as such a response is, then raised with its URL)
        self.srv.route("/odd", (200, {UNPARSABLE: "1"}, b"body"))
        with mock.patch.object(http.client, "parse_headers", unparsable()), self.assertRaises(Exception) as caught:
            frl.request(self.srv.url + "/odd")
        e = caught.exception
        self.assertRegex(f"{type(e).__name__}: {e}", r"^IntegrityError: \S+/odd: a response that cannot be read \(TypeError: ")
        # fails: TypeError: '<' not supported between instances of 'int' and 'NoneType'
        self.assertEqual(self.srv.hits("/odd"), frl.RETRIES)  # fails: 1

    def test_so_is_one_it_fails_on_with_another_exception_of_a_bug(self):
        self.srv.route("/odd", (200, {UNPARSABLE: "1"}, b"body"))
        with mock.patch.object(http.client, "parse_headers", unparsable(error=RecursionError("too deep"))), \
                self.assertRaises(Exception) as caught:
            frl.request(self.srv.url + "/odd")
        e, want = caught.exception, r"^IntegrityError: \S+/odd: a response that cannot be read \(RecursionError: too deep\)$"
        self.assertRegex(f"{type(e).__name__}: {e}", want)  # fails: RecursionError: too deep

    def test_an_interim_one_too(self):
        # (urllib3 reads a proxy's answer to CONNECT with FinalResponse._read_status alone, outside begin)
        class Sock:
            def makefile(self, mode):
                return io.BytesIO(b"HTTP/1.1 103 Early Hints\r\nX-Unparsable: 1\r\n\r\nHTTP/1.1 200 OK\r\n\r\n")
        with mock.patch.object(http.client, "parse_headers", unparsable()), \
                self.assertRaisesRegex(frl.IntegrityError, r"^a response that cannot be read \(TypeError: "):
            frl.FinalResponse(Sock())._read_status()  # fails: TypeError, as it was raised

    def test_control_a_bug_in_the_request_itself_is_raised_as_it_is(self):
        # (only what http.client raises as it parses a response is the response's)
        self.srv.route("/fine", (200, {}, b"body"))
        with self.assertRaises(TypeError):
            frl.request(self.srv.url + "/fine", headers={"X-Generation": None})
        self.assertEqual(self.srv.hits("/fine"), 0)


class HeadersTest(unittest.TestCase):
    def test_every_gcs_hash_header_is_read(self):
        srv = KeepAliveServer()
        self.addCleanup(srv.close)

        def two_hashes(h):
            h.send_response(200)
            h.send_header("x-goog-hash", "crc32c=AAAAAA==")
            h.send_header("x-goog-hash", "md5=XUFAKrxLKna5cZ2REBfFkg==")
            h.send_header("Content-Length", "5")
            h.end_headers()
            if not getattr(h, "head", False):
                h.wfile.write(b"hello")
        srv.route("/obj", two_hashes)
        headers = frl.request(srv.url + "/obj", consume=lambda r: r.headers, method="HEAD")
        self.assertEqual(frl.gcs_hashes(headers), {"crc32c": "00000000", "md5": "5d41402abc4b2a76b9719d911017c592"})
        self.assertEqual(headers.get("Content-Length"), "5")
        self.assertIsNone(headers.get("x-missing"))
        self.assertEqual(headers.get("x-missing", "dflt"), "dflt")


if __name__ == "__main__":
    unittest.main()
