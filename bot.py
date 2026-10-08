import os, re, time, uuid, signal, asyncio, logging, threading
from urllib.parse import urlparse, unquote, quote

import aiofiles, aiohttp, aioboto3
from aiohttp import web
from botocore import UNSIGNED
from botocore.config import Config as BotoConfig
from boto3.s3.transfer import TransferConfig

from todus import ToDusClient2

TODUS_PHONE = os.environ.get("TODUS_PHONE", "5350155246")
TODUS_JWT = os.environ.get("TODUS_JWT")

S3_ENDPOINT = "https://s3.todus.cu"
S3_BUCKET = "stream"
S3_REGION = "us-east-1"

DOWNLOAD_PATH = "/tmp/todus_uploads"
PORT = 10000
MAX_FILE_SIZE = 2000 * 1024 * 1024
QUEUE_WORKERS = 1
CHUNK_SIZE = 16 * 1024 * 1024
WATCHDOG_LIFETIME = 14100
PARALLEL_URL_DOWNLOAD = True
PARALLEL_URL_PARTS = 4
PARALLEL_URL_MIN_SIZE = 50 * 1024 * 1024

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
}

os.makedirs(DOWNLOAD_PATH, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
logging.getLogger("aiohttp.access").setLevel(logging.WARNING)
log = logging.getLogger("bot")

todus_client = ToDusClient2(TODUS_PHONE)
if TODUS_JWT:
    todus_client.token = TODUS_JWT

_status_throttle = {}

_S3_CONFIG = BotoConfig(
    signature_version=UNSIGNED,
    retries={"max_attempts": 3, "mode": "adaptive"},
    max_pool_connections=50,
    connect_timeout=30,
    read_timeout=600,
    s3={"addressing_style": "path"},
)
_TRANSFER_CONFIG = TransferConfig(
    multipart_threshold=8 * 1024 * 1024,
    multipart_chunksize=64 * 1024 * 1024,
    max_concurrency=16,
    use_threads=True,
)
_s3_session = aioboto3.Session()

_loop = None

def jid_to_phone(jid):
    if not jid:
        return None
    return jid.split("@")[0].split("/")[0]

def send(uid, text):
    try:
        todus_client.send_message(uid, text)
    except Exception as e:
        log.warning(f"send a {uid} falló: {e}")

def send_throttled(uid, key, text, interval=3.0):
    now = time.time()
    if now - _status_throttle.get(key, 0) < interval:
        return
    _status_throttle[key] = now
    send(uid, text)

class QueuedJob:
    __slots__ = ("job_id","user_id","kind","url","original_name","task","created_at","cancel_requested")
    def __init__(self, user_id, kind, url=None, original_name=None):
        self.job_id = uuid.uuid4().hex
        self.user_id = user_id
        self.kind = kind
        self.url = url
        self.original_name = original_name
        self.task = None
        self.created_at = time.time()
        self.cancel_requested = False

class JobQueue:
    def __init__(self, n_workers):
        self.queue = asyncio.Queue()
        self.n_workers = n_workers
        self.workers = []
        self.user_pending = {}
        self.active_jobs = {}
        self._lock = asyncio.Lock()

    async def enqueue(self, job):
        async with self._lock:
            if job.user_id in self.user_pending:
                raise ValueError("user_already_queued")
            self.user_pending[job.user_id] = job
            await self.queue.put(job)
            return self.queue.qsize()

    async def mark_active(self, job):
        async with self._lock:
            self.active_jobs[job.job_id] = job

    async def finish(self, job):
        async with self._lock:
            self.active_jobs.pop(job.job_id, None)
            current = self.user_pending.get(job.user_id)
            if current is not None and current.job_id == job.job_id:
                self.user_pending.pop(job.user_id, None)

    def position_of(self, user_id):
        job = self.user_pending.get(user_id)
        if job is None:
            return None
        if job.job_id in self.active_jobs:
            return 0
        for i, queued in enumerate(self.queue._queue):
            if queued.job_id == job.job_id:
                return i + 1
        return None

    def has_user_job(self, user_id):
        return user_id in self.user_pending

    async def cancel_user(self, user_id):
        async with self._lock:
            job = self.user_pending.get(user_id)
            if job is None:
                return "none"
            if job.job_id in self.active_jobs:
                job.cancel_requested = True
                task = job.task
                if task is not None and not task.done():
                    task.cancel()
                return "active"
            self.user_pending.pop(user_id, None)
            items = list(self.queue._queue)
            self.queue._queue.clear()
            for item in items:
                if item.job_id != job.job_id:
                    self.queue._queue.append(item)
            return "queued"

    @property
    def queued_count(self):
        return self.queue.qsize()

    @property
    def active_count(self):
        return len(self.active_jobs)

    def start_workers(self, process_fn):
        for i in range(self.n_workers):
            self.workers.append(asyncio.create_task(self._worker_loop(i, process_fn)))

    async def _worker_loop(self, worker_id, process_fn):
        while True:
            job = await self.queue.get()
            try:
                if job.cancel_requested:
                    continue
                await self.mark_active(job)
                send(job.user_id, "▶️ Procesando tu enlace...")
                job.task = asyncio.create_task(process_fn(job))
                try:
                    await job.task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    log.exception(f"job {job.job_id} falló: {e}")
            except asyncio.CancelledError:
                raise
            finally:
                await self.finish(job)
                self.queue.task_done()

    async def stop_workers(self):
        for w in self.workers:
            w.cancel()
        for w in self.workers:
            try:
                await w
            except asyncio.CancelledError:
                pass
        self.workers.clear()

job_queue = JobQueue(QUEUE_WORKERS)

def format_size(b):
    if b < 1024: return f"{b} B"
    if b < 1048576: return f"{b / 1024:.1f} KB"
    if b < 1073741824: return f"{b / 1048576:.1f} MB"
    return f"{b / 1073741824:.2f} GB"

def progress_bar(p):
    filled = round(15 * p / 100)
    return "⬢" * filled + "⬡" * (15 - filled)

URL_RE = re.compile(r"(https?://[^\s<>\"']+?)(?=[.,;:!?)\]]?(\s|$))", re.IGNORECASE)

def get_filename_from_url(url):
    try:
        name = os.path.basename(urlparse(url).path)
        if name and len(name) > 2:
            return unquote(name)
    except Exception:
        pass
    return None

def sanitize_filename(name):
    name = name.replace("/", "_").replace("\\", "_")
    name = re.sub(r"[\s?#&]+", "_", name)
    return name.strip("._") or f"file_{int(time.time())}"

def check_disk_space():
    st = os.statvfs(DOWNLOAD_PATH)
    free = st.f_bavail * st.f_frsize
    if free < 2 * 1024 * 1024 * 1024:
        raise RuntimeError(f"Disco insuficiente: {format_size(free)} libres")

async def subir_a_s3(temp_path, filename, size, uid, job_id):
    safe_name = sanitize_filename(filename)
    remote_key = f"{uuid.uuid4().hex[:8]}_{safe_name}"
    loop = asyncio.get_running_loop()
    last_update = [0.0]
    key = f"up:{uid}:{job_id}"

    def _progress_callback(bytes_transferred):
        now = loop.time()
        if now - last_update[0] < 3.0 and bytes_transferred < size:
            return
        last_update[0] = now
        pct = int(bytes_transferred / size * 100) if size else 0
        asyncio.run_coroutine_threadsafe(
            asyncio.to_thread(
                send_throttled, uid, key,
                f"☁️ Subiendo a toDus S3... {pct}%\n[{progress_bar(pct)}]",
                3.0,
            ),
            loop,
        )

    async with _s3_session.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id="public",
        aws_secret_access_key="public",
        region_name=S3_REGION,
        config=_S3_CONFIG,
    ) as s3:
        with open(temp_path, "rb") as f:
            await s3.upload_fileobj(
                f, S3_BUCKET, remote_key,
                ExtraArgs={"ContentType": "application/octet-stream"},
                Config=_TRANSFER_CONFIG,
                Callback=_progress_callback,
            )
    return f"{S3_ENDPOINT}/{S3_BUCKET}/{quote(remote_key)}"

async def process_job(job):
    await _process_url(job)

async def _download_sequential(session, url, temp_path, total, job):
    downloaded = 0
    last_pct = -1
    key = f"dl:{job.user_id}:{job.job_id}"
    async with aiofiles.open(temp_path, "wb") as f:
        async with session.get(url, headers=BROWSER_HEADERS) as resp:
            if resp.status >= 400:
                raise RuntimeError(f"HTTP {resp.status}")
            async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                await f.write(chunk)
                downloaded += len(chunk)
                if downloaded > MAX_FILE_SIZE:
                    raise RuntimeError("Archivo supera el límite")
                if total:
                    pct = int(downloaded / total * 100)
                    if pct - last_pct >= 10 or pct == 100:
                        last_pct = pct
                        send_throttled(
                            job.user_id, key,
                            f"📥 Descargando... {pct}%\n[{progress_bar(pct)}]\n"
                            f"{format_size(downloaded)}/{format_size(total)}",
                            3.0,
                        )
    return downloaded

async def _download_parallel(session, url, temp_path, total, job):
    n_parts = PARALLEL_URL_PARTS
    part_size = total // n_parts
    parts = []
    for i in range(n_parts):
        start = i * part_size
        end = start + part_size - 1 if i < n_parts - 1 else total - 1
        parts.append((i, start, end))
    progress = {"total": 0}
    progress_lock = asyncio.Lock()
    last_pct = [-1]
    key = f"dl:{job.user_id}:{job.job_id}"

    async def download_part(idx, start, end):
        path = f"{temp_path}.part{idx}"
        headers = dict(BROWSER_HEADERS)
        headers["Range"] = f"bytes={start}-{end}"
        async with session.get(url, headers=headers) as resp:
            if resp.status not in (200, 206):
                raise RuntimeError(f"HTTP {resp.status} en parte {idx}")
            async with aiofiles.open(path, "wb") as f:
                async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                    await f.write(chunk)
                    async with progress_lock:
                        progress["total"] += len(chunk)
                        pct = int(progress["total"] / total * 100)
                        if pct - last_pct[0] >= 10 or pct == 100:
                            last_pct[0] = pct
                            send_throttled(
                                job.user_id, key,
                                f"📥 Descargando (x{n_parts})... {pct}%\n"
                                f"[{progress_bar(pct)}]",
                                3.0,
                            )
        return path

    part_paths = await asyncio.gather(*[download_part(i, s, e) for i, s, e in parts])
    async with aiofiles.open(temp_path, "wb") as out:
        for p in part_paths:
            async with aiofiles.open(p, "rb") as src:
                while True:
                    chunk = await src.read(8 * 1024 * 1024)
                    if not chunk:
                        break
                    await out.write(chunk)
            try:
                os.unlink(p)
            except Exception:
                pass
    return total

async def _process_url(job):
    url = job.url
    filename = sanitize_filename(get_filename_from_url(url) or f"file_{int(time.time())}")
    ext = os.path.splitext(filename)[1] or ".bin"
    temp_path = os.path.join(DOWNLOAD_PATH, f"{uuid.uuid4().hex}{ext}")
    try:
        check_disk_space()
        async with aiohttp.ClientSession() as session:
            async with asyncio.timeout(1200):
                total = 0
                accepts_ranges = False
                try:
                    async with session.head(url, headers=BROWSER_HEADERS) as h:
                        if h.status < 400:
                            total = int(h.headers.get("Content-Length", 0))
                            accepts_ranges = h.headers.get("Accept-Ranges", "").lower() == "bytes"
                except Exception:
                    pass
                if total and total > MAX_FILE_SIZE:
                    raise RuntimeError(f"Archivo {format_size(total)} supera el límite")
                if PARALLEL_URL_DOWNLOAD and accepts_ranges and total >= PARALLEL_URL_MIN_SIZE:
                    await _download_parallel(session, url, temp_path, total, job)
                else:
                    await _download_sequential(session, url, temp_path, total, job)

        size = os.path.getsize(temp_path)
        send(job.user_id, f"✅ Descarga completa: {filename}\n📊 {format_size(size)}\n☁️ Subiendo a toDus S3...")

        url_final = await subir_a_s3(temp_path, filename, size, job.user_id, job.job_id)

        name = os.path.splitext(filename)[0].replace("_", " ")
        ext_out = os.path.splitext(filename)[1].replace(".", "")
        send(job.user_id,
            f"┎ NAME: {name}\n"
            f"┠ EXTENSION: {ext_out}\n"
            f"┠ SIZE: {format_size(size)}\n"
            f"┠ CLOUD: toDus S3\n"
            f"┖ URL: {url_final}"
        )
    except asyncio.CancelledError:
        send(job.user_id, "❌ Cancelado")
        raise
    except Exception as e:
        log.exception("error procesando URL")
        send(job.user_id, f"❌ Error: {str(e)[:200]}")
    finally:
        try:
            os.unlink(temp_path)
        except Exception:
            pass

def on_todus_message(msg):
    try:
        from_jid = msg.get("from")
        body = msg.get("body", "").strip()
        if not from_jid or not body:
            return
        uid = jid_to_phone(from_jid)
        if not uid:
            return
        log.info(f"📩 de {uid}: {body[:80]}")

        low = body.lower()

        if low in ("/start", "hola", "ayuda", "help"):
            send(uid,
                "🤖 Bot de subida a toDus S3\n\n"
                "Envíame un enlace (http/https) y lo descargaré, "
                "lo subiré a toDus S3 y te devolveré la URL.\n\n"
                "Comandos:\n"
                "/cancel — cancelar tu trabajo\n"
                "/status — ver tu posición en la cola"
            )
            return

        if low == "/cancel" or low == "cancelar":
            result = asyncio.run_coroutine_threadsafe(
                job_queue.cancel_user(uid), _loop
            ).result(timeout=5)
            if result == "none":
                send(uid, "ℹ️ No tienes trabajos activos.")
            elif result == "active":
                send(uid, "❌ Cancelando tu trabajo activo...")
            else:
                send(uid, "✅ Removido de la cola.")
            return

        if low == "/status":
            pos = job_queue.position_of(uid)
            tq = job_queue.queued_count
            ta = job_queue.active_count
            if pos is None:
                send(uid, f"ℹ️ Sin trabajos.\n📊 Cola: {tq} esperando, {ta} procesando")
            elif pos == 0:
                send(uid, f"▶️ Procesándose ahora.\n📊 Cola: {tq} esperando, {ta} procesando")
            else:
                send(uid, f"⏳ Posición #{pos} de la cola.\n📊 Cola: {tq} esperando, {ta} procesando")
            return

        match = URL_RE.search(body)
        if not match:
            send(uid, "Envía un enlace (http/https) para procesar.")
            return

        if job_queue.has_user_job(uid):
            send(uid, "⚠️ Ya tienes un trabajo en curso.")
            return

        url = match.group(1)
        job = QueuedJob(user_id=uid, kind="url", url=url)
        try:
            position = asyncio.run_coroutine_threadsafe(
                job_queue.enqueue(job), _loop
            ).result(timeout=5)
        except ValueError:
            send(uid, "⚠️ Ya tienes un trabajo en curso.")
            return
        send(uid, f"📥 En cola — posición #{position}")
    except Exception as e:
        log.exception(f"on_todus_message: {e}")

START_TIME = time.time()

async def health_handler(request):
    return web.json_response({
        "status": "healthy",
        "uptime": round(time.time() - START_TIME, 1),
        "queue_queued": job_queue.queued_count,
        "queue_active": job_queue.active_count,
    })

async def root_handler(request):
    return web.json_response({"status": "online"})

def make_web_app():
    a = web.Application()
    a.router.add_get("/", root_handler)
    a.router.add_get("/health", health_handler)
    return a

async def run_web():
    a = make_web_app()
    runner = web.AppRunner(a)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    return runner

async def watchdog():
    while True:
        await asyncio.sleep(30)
        uptime = time.time() - START_TIME
        if uptime > WATCHDOG_LIFETIME:
            log.warning(f"watchdog: {uptime:.0f}s, SIGTERM")
            os.kill(os.getpid(), signal.SIGTERM)
            await asyncio.sleep(10)
            log.error("watchdog: forzando os._exit")
            os._exit(0)

async def main():
    global _loop
    _loop = asyncio.get_running_loop()

    web_runner = await run_web()
    job_queue.start_workers(process_job)
    asyncio.create_task(watchdog())

    todus_client.login_with_phone_only()
    log.info(f"✅ toDus login OK — {TODUS_PHONE}")

    listener_thread = threading.Thread(
        target=lambda: todus_client.listen_messages(on_todus_message),
        daemon=True,
    )
    listener_thread.start()

    log.info(f"BOT READY — {QUEUE_WORKERS} workers, límite {format_size(MAX_FILE_SIZE)}")
    try:
        await asyncio.Event().wait()
    finally:
        log.info("apagando limpiamente...")
        await job_queue.stop_workers()
        await web_runner.cleanup()
        log.info("apagado completo")

if __name__ == "__main__":
    asyncio.run(main())
