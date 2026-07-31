"""Regenerate PUBLOG_SOURCE_MANIFEST in pub_tools/assets.py.

Discovers the CSV members of each PUB LOG zip WITHOUT downloading the whole
archive, by handing zipfile a seekable file-like backed by HTTP Range requests:
only the End-of-Central-Directory and central directory are fetched, a few KiB
instead of gigabytes.

Run it when ingest warns about manifest drift, or when adding a source:

    python scripts/discover_manifest.py

It prints a paste-ready Python dict for pub_tools/assets.py. Anything it cannot
read is reported per-URL rather than silently omitted -- an entry missing from
the manifest means missing asset keys, so failures must be loud.
"""
import io
import sys
import zipfile

from pub_tools.assets import (
    PUBLOG_MONTHLY_URLS,
    PUBLOG_QUARTERLY_URLS,
    source_filename,
    publog_session,
)


class HttpRangeFile(io.RawIOBase):
    def __init__(self, session, url):
        self.session = session
        self.url = url
        self._pos = 0
        # HEAD is blocked per the module docstring, so probe with a 1-byte GET.
        # A 206 + Content-Range is the only proof that matters; Accept-Ranges is
        # advisory and this CDN omits it even when ranges work.
        r = session.get(url, headers={"Range": "bytes=0-0"}, timeout=30, stream=True)
        r.raise_for_status()
        cr = r.headers.get("Content-Range", "")
        r.close()
        if r.status_code != 206 or "/" not in cr:
            raise RuntimeError(
                "server ignores Range (status %s, Content-Range %r)" % (r.status_code, cr)
            )
        self._size = int(cr.rsplit("/", 1)[1])

    def seekable(self):
        return True

    def readable(self):
        return True

    def seek(self, offset, whence=0):
        if whence == 0:
            self._pos = offset
        elif whence == 1:
            self._pos += offset
        else:
            self._pos = self._size + offset
        return self._pos

    def tell(self):
        return self._pos

    def read(self, size=-1):
        if size < 0:
            size = self._size - self._pos
        if size == 0 or self._pos >= self._size:
            return b""
        end = min(self._pos + size, self._size) - 1
        r = self.session.get(
            self.url, headers={"Range": "bytes=%d-%d" % (self._pos, end)}, timeout=60
        )
        r.raise_for_status()
        data = r.content
        self._pos += len(data)
        return data

    def readinto(self, b):
        # BufferedReader drives RawIOBase through readinto, not read.
        data = self.read(len(b))
        b[: len(data)] = data
        return len(data)


def main():
    session = publog_session()
    manifest = {}
    failures = []
    for url in PUBLOG_MONTHLY_URLS + PUBLOG_QUARTERLY_URLS:
        name = source_filename(url)
        if not name.lower().endswith(".zip"):
            manifest[name] = [name]
            print("%-24s bare csv" % name, file=sys.stderr, flush=True)
            continue
        try:
            with HttpRangeFile(session, url) as raw:
                with zipfile.ZipFile(io.BufferedReader(raw, buffer_size=1 << 16)) as z:
                    members = sorted(
                        n for n in z.namelist()
                        if n.lower().endswith(".csv") and not n.endswith("/")
                    )
            if not members:
                raise RuntimeError("archive contains no CSV members")
            manifest[name] = members
            print("%-24s %2d CSV(s)" % (name, len(members)), file=sys.stderr, flush=True)
        except Exception as e:
            failures.append((name, "%s: %s" % (type(e).__name__, e)))
            print("%-24s FAILED: %s: %s" % (name, type(e).__name__, e),
                  file=sys.stderr, flush=True)

    print("PUBLOG_SOURCE_MANIFEST: Dict[str, List[str]] = {")
    for name, members in manifest.items():
        print("    %r: [" % name)
        for m in members:
            print("        %r," % m)
        print("    ],")
    print("}")

    if failures:
        print("\n%d source(s) could not be read; manifest is INCOMPLETE:"
              % len(failures), file=sys.stderr)
        for name, err in failures:
            print("   %-24s %s" % (name, err), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
