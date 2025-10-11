# main_bot.py
import os
import re
import io
import json
import asyncio
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Optional, List, Tuple

import discord
from discord.ext import commands

import aiohttp
import requests
import httpx

import firebase_admin
from firebase_admin import credentials, firestore

# =========================
# ENV & FIREBASE INIT
# =========================
TOKEN = os.getenv("DISCORD_BOT_TOKEN")
if not TOKEN:
    print("❌ Env DISCORD_BOT_TOKEN tidak ditemukan.")

firebase_json = os.getenv("FIREBASE_SERVICE_ACCOUNT_JSON")
if not firebase_json:
    print("❌ Env FIREBASE_SERVICE_ACCOUNT_JSON tidak ditemukan.")
    raise SystemExit(1)

try:
    cred = credentials.Certificate(json.loads(firebase_json))
    firebase_admin.initialize_app(cred)
    db = firestore.client()
    print("✅ Firestore terhubung.")
except Exception as e:
    print(f"❌ Gagal inisialisasi Firestore: {e}")
    raise

# =========================
# KONFIG DISCORD / ID
# =========================
CHANNEL_ID_WELCOME          = 1423964756158447738
CHANNEL_ID_LOGS             = 1423969192389902339    # moderator/log channel
CHANNEL_ID_MABAR            = 1424029336683679794
CHANNEL_ID_INTRO            = 1424033383339659334
RULES_CHANNEL_ID            = 1423969192389902336
ROLE_ID_LIGHT               = 1424026593143164958
CHANNEL_ID_PHOTO_MEDIA      = 1424033929874247802    # forward foto/ gambar
CHANNEL_ID_DOWNLOADER       = 1425023771185774612    # channel downloader
CHANNEL_ID_LINK_DETECT      = 1424032583519567952    # deteksi link → arahkan ke downloader
CHANNEL_ID_SERVER_SPOTLIGHT = 1425015637197066260    # tujuan announce

REACTION_EMOJI = "🔆"
TZ = ZoneInfo("Asia/Jakarta")

intents = discord.Intents.default()
intents.members = True
intents.guilds = True
intents.message_content = True
intents.reactions = True

bot = commands.Bot(command_prefix="!", intents=intents)
KONTEN_LIMIT = 1000
MAX_UPLOAD_BYTES = 25 * 1024 * 1024  # 25 MB
URL_ANY = re.compile(r"(https?://\S+)", re.IGNORECASE)

# =========================
# UTIL TIME
# =========================
def now_wib() -> datetime:
    return datetime.now(TZ)

def to_epoch(dt: datetime) -> float:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ)
    return dt.astimezone(timezone.utc).timestamp()

def from_epoch_to_wib(epoch: float) -> datetime:
    return datetime.fromtimestamp(epoch, tz=TZ)

# =========================
# PARSER WAKTU (WIB)
# =========================
def parse_natural_time(text: str, ref: datetime):
    t = text.lower().strip()
    if t in {"now", "sekarang", "skrng"}:
        return ref, "sekarang (WIB)"

    is_tomorrow = "besok" in t
    m = re.search(r"(\d{1,2})(?:[:.](\d{1,2}))?", t)
    hour, minute = 0, 0
    if m:
        hour = int(m.group(1))
        minute = int(m.group(2) or 0)

    pagi  = "pagi" in t
    siang = "siang" in t
    sore  = "sore" in t
    malam = "malam" in t

    if pagi and hour == 12:
        hour = 0
    elif (sore or malam) and hour < 12:
        hour += 12

    target = ref.replace(hour=hour % 24, minute=minute, second=0, microsecond=0)
    if is_tomorrow or target <= ref:
        target += timedelta(days=1)

    return target, target.strftime("%H:%M WIB")

# =========================
# FIRESTORE HELPERS  (disesuaikan dgn struktur: config/downloader)
# =========================
WELCOME_COL   = "welcome_messages"
MABAR_COL     = "mabar_reminders"
CONFIG_COL    = "config"
DL_DOC_ID     = "downloader"          # fields: status ("on"/"off"), info_msg (int), updated (string)
ANNOUNCE_COL  = "announcements"

async def save_welcome_message(user_id: int, message_id: int):
    try:
        db.collection(WELCOME_COL).document(str(user_id)).set({
            "message_id": message_id,
            "created_at": firestore.SERVER_TIMESTAMP
        })
    except Exception as e:
        print("[WARN] save_welcome_message:", e)

async def get_welcome_message(user_id: int) -> Optional[int]:
    try:
        doc = db.collection(WELCOME_COL).document(str(user_id)).get()
        if doc.exists:
            return int(doc.to_dict().get("message_id") or 0) or None
    except Exception as e:
        print("[WARN] get_welcome_message:", e)
    return None

async def delete_welcome_message(user_id: int):
    try:
        db.collection(WELCOME_COL).document(str(user_id)).delete()
    except Exception as e:
        print("[WARN] delete_welcome_message:", e)

def save_mabar_schedule(doc_id: str, data: dict):
    try:
        db.collection(MABAR_COL).document(doc_id).set(data)
    except Exception as e:
        print("[WARN] save_mabar_schedule:", e)

def update_mabar_status(doc_id: str, **fields):
    try:
        db.collection(MABAR_COL).document(doc_id).update(fields)
    except Exception as e:
        print("[WARN] update_mabar_status:", e)

def load_pending_mabar(now_epoch: float):
    try:
        q = db.collection(MABAR_COL).where("status", "==", "scheduled").stream()
        items = []
        for d in q:
            dat = d.to_dict()
            if "remind_at_epoch" in dat and "guild_id" in dat and "channel_id" in dat and "map_name" in dat:
                if dat["remind_at_epoch"] + 5400 > now_epoch:
                    items.append((d.id, dat))
        return items
    except Exception as e:
        print("[WARN] load_pending_mabar:", e)
        return []

def _dl_ref():
    return db.collection(CONFIG_COL).document(DL_DOC_ID)

def get_downloader_config() -> dict:
    try:
        snap = _dl_ref().get()
        return snap.to_dict() if snap.exists else {}
    except Exception as e:
        print("[WARN] get_downloader_config:", e)
        return {}

def get_downloader_enabled(guild_id: int) -> bool:
    # guild_id tidak dipakai di struktur baru; tetap ada utk kompatibilitas
    cfg = get_downloader_config()
    return str(cfg.get("status", "on")).lower() == "on"

def set_downloader_status(on: bool):
    try:
        _dl_ref().set(
            {"status": "on" if on else "off", "updated": now_wib().isoformat()},
            merge=True
        )
    except Exception as e:
        print("[WARN] set_downloader_status:", e)

def get_downloader_notice_id() -> Optional[int]:
    try:
        cfg = get_downloader_config()
        mid = cfg.get("info_msg")
        return int(mid) if isinstance(mid, (int, float, str)) and str(mid).isdigit() else None
    except Exception as e:
        print("[WARN] get_downloader_notice_id:", e)
        return None

def set_downloader_notice_id(message_id: int):
    try:
        _dl_ref().set({"info_msg": int(message_id), "updated": now_wib().isoformat()}, merge=True)
    except Exception as e:
        print("[WARN] set_downloader_notice_id:", e)

def log_announcement(data: dict):
    try:
        db.collection(ANNOUNCE_COL).add({**data, "created_at": firestore.SERVER_TIMESTAMP})
    except Exception as e:
        print("[WARN] log_announcement:", e)

# =========================
# STARTUP
# =========================
@bot.event
async def on_ready():
    print(f"✅ Bot login sebagai {bot.user}")
    try:
        await bot.change_presence(activity=discord.Game("menjaga server ✨"))
    except Exception:
        pass

    # Resume reminders
    pending = load_pending_mabar(to_epoch(now_wib()))
    if pending:
        print(f"⏲️ Menjadwalkan ulang {len(pending)} reminder mabar dari Firestore.")
    for doc_id, dat in pending:
        asyncio.create_task(schedule_mabar_tasks_from_doc(doc_id, dat))

    # Pastikan notice downloader tidak duplikat
    await ensure_downloader_notice()

async def _build_downloader_embed(enabled: bool) -> discord.Embed:
    status_bullet = "🟢" if enabled else "🔴"
    desc = (
        "Cukup !dw untuk mulai men-download —\n"
        "bot akan otomatis membuat **thread pribadi** khusus untukmu 🤫\n"
        "(hanya kamu dan bot yang dapat melihat percakapan tersebut).\n\n"
        "📦 **Maksimum ukuran media: 25 MB**\n"
        "Lebih dari itu, bot akan mengirimkan tautan unduhan.\n"
        f"| {status_bullet} Fitur ini aktif untuk member dengan role 🔆 Light."
    )
    embed = discord.Embed(title="Downloader Center", description=desc, color=discord.Color.blurple())
    return embed

async def ensure_downloader_notice():
    ch = bot.get_channel(CHANNEL_ID_DOWNLOADER)
    if not isinstance(ch, discord.TextChannel):
        return
    enabled = get_downloader_enabled(ch.guild.id if ch.guild else 0)
    embed = await _build_downloader_embed(enabled)

    msg_id = get_downloader_notice_id()
    if msg_id:
        # Coba edit. Jika tidak ada (terhapus), kirim ulang.
        try:
            msg = await ch.fetch_message(msg_id)
            await msg.edit(embed=embed)
            return
        except Exception:
            pass

    # Kirim baru dan simpan id
    msg = await ch.send(embed=embed)
    set_downloader_notice_id(msg.id)

# =========================
# GREETINGS + REACTION ROLE
# =========================
@bot.event
async def on_member_join(member: discord.Member):
    ch = bot.get_channel(CHANNEL_ID_WELCOME)
    if not isinstance(ch, discord.TextChannel):
        return

    rules_ch = member.guild.get_channel(RULES_CHANNEL_ID) if member.guild else None
    rules_text = rules_ch.mention if isinstance(rules_ch, discord.TextChannel) else "#rules"

    role_light = member.guild.get_role(ROLE_ID_LIGHT) if member.guild else None
    role_text = role_light.mention if role_light else "**Light**"

    desc = (
        f"Halo {member.mention}, selamat datang di **{member.guild.name}**!\n"
        f"• Baca aturan di {rules_text}\n"
        f"• Klik reaksi {REACTION_EMOJI} di pesan ini untuk **ambil role {role_text}**.\n"
        f"• Klik ulang untuk melepas role."
    )

    embed = discord.Embed(
        title="🎉 Selamat Datang!",
        description=desc,
        color=discord.Color.green()
    )
    embed.set_footer(text="Selamat bergabung & have fun! ✨")

    msg = await ch.send(embed=embed)
    try:
        await msg.add_reaction(REACTION_EMOJI)
    except Exception:
        pass

    await save_welcome_message(member.id, msg.id)

    async def autodel():
        await asyncio.sleep(24 * 3600)
        stored_id = await get_welcome_message(member.id)
        if stored_id and stored_id == msg.id:
            try: await msg.delete()
            except Exception: pass
            await delete_welcome_message(member.id)

    asyncio.create_task(autodel())

@bot.event
async def on_member_remove(member: discord.Member):
    await delete_welcome_message(member.id)
    ch = bot.get_channel(CHANNEL_ID_WELCOME)
    if not isinstance(ch, discord.TextChannel):
        return
    embed = discord.Embed(
        title="👋 Selamat Tinggal",
        description=f"{member.display_name} telah keluar dari server.",
        color=discord.Color.red()
    )
    await ch.send(embed=embed)

async def _safe_get_member(guild: discord.Guild, user_id: int) -> Optional[discord.Member]:
    m = guild.get_member(user_id)
    if m is None:
        try:
            m = await guild.fetch_member(user_id)
        except Exception:
            m = None
    return m

@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if payload.guild_id is None or str(payload.emoji) != REACTION_EMOJI:
        return
    target_msg_id = await get_welcome_message(payload.user_id)
    if not target_msg_id or payload.message_id != target_msg_id:
        return

    guild = bot.get_guild(payload.guild_id)
    if not guild:
        return
    member = await _safe_get_member(guild, payload.user_id)
    if not member or member.bot:
        return
    role = guild.get_role(ROLE_ID_LIGHT)
    if not role:
        return
    channel = bot.get_channel(CHANNEL_ID_WELCOME)

    try:
        if role not in member.roles:
            await member.add_roles(role, reason="Welcome role Light")
            intro_channel = guild.get_channel(CHANNEL_ID_INTRO)
            if isinstance(intro_channel, discord.TextChannel):
                await intro_channel.send(
                    f"Ekhem… {member.mention}! Sebutin umur kamu aja boleh kok. "
                    f"Kalau mau cerita lebih, juga boleh, ngga perlu terlalu detail, ya!"
                )
        else:
            await member.remove_roles(role, reason="Remove role Light")
    except Exception as e:
        print("[ERROR] Toggle role:", e)
        return

    try:
        if isinstance(channel, discord.TextChannel):
            msg = await channel.fetch_message(target_msg_id)
            have_role = role in member.roles
            status = "✅ Role Light diberikan." if have_role else "❎ Role Light dilepas."
            new_embed = msg.embeds[0] if msg.embeds else discord.Embed(color=discord.Color.green())
            new_embed.set_footer(text=status + " (pesan akan dihapus sebentar lagi)")
            await msg.edit(embed=new_embed)
            await asyncio.sleep(8)
            await msg.delete()
    except Exception:
        pass
    finally:
        await delete_welcome_message(member.id)

# =========================
# LOG PESAN DIHAPUS
# =========================
@bot.event
async def on_message_delete(message: discord.Message):
    if message.author.bot:
        return
    log_channel = bot.get_channel(CHANNEL_ID_LOGS)
    if not isinstance(log_channel, discord.TextChannel):
        return
    raw = (message.content or "")
    konten = raw[:KONTEN_LIMIT] + ("..." if len(raw) > KONTEN_LIMIT else "")
    konten = konten.replace("```", "")

    embed = discord.Embed(title="🗑️ Pesan Dihapus", color=discord.Color.orange())
    embed.add_field(name="Pengirim", value=message.author.mention, inline=False)
    if isinstance(message.channel, (discord.TextChannel, discord.Thread)):
        embed.add_field(name="Channel", value=message.channel.mention, inline=False)
    if konten.strip():
        embed.add_field(name="Konten", value=f"```{konten}```", inline=False)
    await log_channel.send(embed=embed)

# =========================
# FORWARD GAMBAR DGN KONFIRMASI
# =========================
IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".gif", ".webp"}

def _is_image_attachment(att: discord.Attachment) -> bool:
    ct = (att.content_type or "").lower()
    if ct.startswith("image/"):
        return True
    name = (att.filename or "").lower()
    return any(name.endswith(ext) for ext in IMAGE_EXTS)

def _jump_url(guild_id: int, channel_id: int, message_id: int) -> str:
    return f"https://discord.com/channels/{guild_id}/{channel_id}/{message_id}"

async def _confirm_and_forward_images(message: discord.Message):
    if not message.guild or not message.attachments:
        return
    images = [att for att in message.attachments if _is_image_attachment(att)]
    if not images:
        return

    prompt = await message.channel.send(
        f"hola {message.author.mention}, apakah kamu ingin fotonya aku forward ke **Channel Photo-Media**?"
    )
    async def timeout_cleanup():
        await asyncio.sleep(30)
        try: await prompt.delete()
        except Exception: pass
    timeout_task = asyncio.create_task(timeout_cleanup())
    for em in ("✅", "❌"):
        try: await prompt.add_reaction(em)
        except Exception: pass

    def check(reaction, user):
        return user == message.author and str(reaction.emoji) in ["✅", "❌"] and reaction.message.id == prompt.id

    try:
        reaction, _ = await bot.wait_for("reaction_add", timeout=30.0, check=check)
        try: timeout_task.cancel()
        except Exception: pass
        try: await prompt.delete()
        except Exception: pass
    except asyncio.TimeoutError:
        await message.channel.send("⏰ Konfirmasi habis. Forward dibatalkan.", delete_after=6)
        return

    if str(reaction.emoji) == "❌":
        await message.channel.send("❌ Oke, tidak di-forward.", delete_after=5)
        return

    try:
        dest = bot.get_channel(CHANNEL_ID_PHOTO_MEDIA)
        if not isinstance(dest, discord.TextChannel):
            return await message.channel.send("⚠️ Channel Photo-Media tidak ditemukan.", delete_after=6)
        caption = message.clean_content.strip()
        prefix = f"media dari {message.author.mention}"
        content = f"{prefix}\n{caption}" if caption else prefix

        files = []
        for att in images[:10]:
            try:
                files.append(await att.to_file())
            except Exception as e:
                print("[WARN] to_file:", e)

        sent = await dest.send(content=content, files=files) if files else await dest.send(content)
        jump = _jump_url(message.guild.id, dest.id, sent.id) if sent else ""
        await message.channel.send(
            f"Ekhem.. media {message.author.mention} udah aku forward ke "
            f"[Media Photo]({jump}), cuss lihat~",
            suppress_embeds=True,
            delete_after=10
        )
    except Exception as e:
        print("[ERROR] forward foto:", e)
        await message.channel.send("⚠️ Terjadi kendala saat forward media.", delete_after=6)

# =========================
# DOWNLOADER (API: dl.siputzx.my.id) + CAROUSEL IG
# =========================
class DlActionView(discord.ui.View):
    def __init__(self, thread: discord.Thread, author_id: int):
        super().__init__(timeout=300)
        self.thread = thread
        self.author_id = author_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("Tombol ini hanya untuk pembuat thread.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="🌸 Download lagi", style=discord.ButtonStyle.primary)
    async def again(self, interaction: discord.Interaction, _: discord.ui.Button):
        await interaction.response.send_message(
            "Kirim tautan sosial (TikTok / Instagram / dll) di thread ini.\n🔒 Aman — hanya kamu & bot yang bisa melihat.",
            ephemeral=True
        )

    @discord.ui.button(label="🌸 Tutup thread", style=discord.ButtonStyle.secondary)
    async def close(self, interaction: discord.Interaction, _: discord.ui.Button):
        try:
            await self.thread.edit(archived=True, locked=True)
            await interaction.response.send_message("✅ Thread ditutup.", ephemeral=True)
        except Exception:
            await interaction.response.send_message("❌ Gagal menutup thread.", ephemeral=True)

async def ensure_private_thread(channel: discord.TextChannel, user: discord.Member) -> discord.Thread:
    name = f"DL-{user.display_name}".strip()[:80]
    th = await channel.create_thread(name=name, type=discord.ChannelType.private_thread, invitable=False)
    try:
        await th.add_user(user)
    except Exception:
        pass
    return th

async def post_siputzx(link: str) -> tuple[dict | None, str | None]:
    """Auto deteksi TikTok / Instagram lalu POST ke API dl.siputzx.my.id"""
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    payload = {"url": link}
    if "tiktok" in link:
        payload["videoQuality"] = "1080"
        payload["downloadMode"] = "auto"
    elif "instagram" in link:
        payload["videoQuality"] = "720"
        payload["audioFormat"] = "mp3"

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post("https://dl.siputzx.my.id/", headers=headers, json=payload)
        if resp.status_code != 200:
            return None, f"HTTP {resp.status_code}"
        return resp.json(), None
    except Exception as e:
        return None, str(e)

def _headers_for_url(url: str) -> dict:
    ref = "https://dl.siputzx.my.id/"
    if "instagram" in url or "cdninstagram" in url:
        ref = "https://www.instagram.com/"
    elif "tiktok" in url or "tiktokcdn" in url:
        ref = "https://www.tiktok.com/"
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/127.0.0.1 Safari/537.36",
        "Referer": ref,
        "Accept": "*/*"
    }

async def download_bytes(url: str, max_bytes: int = 25_000_000) -> tuple[bytes | None, bool]:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=_headers_for_url(url), timeout=aiohttp.ClientTimeout(total=90)) as r:
                if r.status != 200:
                    print(f"[download] {r.status} {url[:80]}")
                    return None, True
                total = 0
                buff = io.BytesIO()
                async for chunk in r.content.iter_chunked(256 * 1024):
                    total += len(chunk)
                    if total > max_bytes:
                        return None, True
                    buff.write(chunk)
                return buff.getvalue(), False
    except Exception as e:
        print("[download] error:", e)
        return None, True

async def send_media_or_link(thread: discord.Thread, author: discord.Member, url: str, filename: str):
    content, fail = await download_bytes(url)
    if fail or not content:
        await thread.send(f"⚠️ File terlalu besar atau gagal unduh.\n🔗 {url}", view=DlActionView(thread, author.id))
        return
    file = discord.File(io.BytesIO(content), filename)
    await thread.send(content=f"📦 Media untuk {author.mention}", file=file, view=DlActionView(thread, author.id))

async def process_download_in_thread(thread: discord.Thread, author: discord.Member, link: str):
    await thread.send("⏳ Sedang mengambil media dari tautan...")

    data, err = await post_siputzx(link)
    if not data:
        await thread.send(f"❌ Gagal ambil data: {err}", view=DlActionView(thread, author.id))
        return

    status = data.get("status", "")
    filename = data.get("filename", "media.mp4")
    url = data.get("url")

    # CASE 1 - Carousel Instagram
    if status == "picker" and isinstance(data.get("picker"), list):
        for i, item in enumerate(data["picker"], start=1):
            img_url = item.get("thumb") or item.get("url")
            if not img_url:
                continue
            fname = f"ig_item_{i:02d}.jpg"
            await send_media_or_link(thread, author, img_url, fname)
        await thread.send("✅ Semua media dari carousel sudah dikirim.", view=DlActionView(thread, author.id))
        return

    # CASE 2 - TikTok atau Instagram Feed / Reels
    if url:
        await send_media_or_link(thread, author, url, filename)
        return

    await thread.send("❌ Tidak menemukan media yang bisa diunduh.", view=DlActionView(thread, author.id))

# =========================
# on_message (SATU-SATUNYA)
# =========================
@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return

    # A) Deteksi link di CHANNEL_ID_LINK_DETECT → arahkan ke downloader (hapus 5 menit)
    if message.channel.id == CHANNEL_ID_LINK_DETECT:
        if URL_ANY.search(message.content):
            ch = bot.get_channel(CHANNEL_ID_DOWNLOADER)
            if isinstance(ch, discord.TextChannel):
                tip = await message.reply(
                    f"hola {message.author.mention}, mau download medianya? ke {ch.mention} yuk!",
                    mention_author=True
                )
                try:
                    await tip.delete(delay=300)
                except Exception:
                    pass

    # B) Downloader channel: kalau user kirim link langsung → hapus & minta pakai !dw
    if message.channel.id == CHANNEL_ID_DOWNLOADER and not isinstance(message.channel, discord.Thread):
        if URL_ANY.search(message.content):
            try:
                await message.delete()
            except Exception:
                pass
            await message.channel.send(
                f"{message.author.mention} demi privasi, gunakan perintah **`!dw`** dulu untuk membuat thread privat, ya.",
                delete_after=30
            )

    # C) Deteksi !mabar / !main (manual)
    content_low = message.content.lower()
    match_cmd = re.search(r'!(mabar|main)\s+(.+)', content_low)
    if match_cmd:
        ctx = await bot.get_context(message)
        arg = match_cmd.group(2).strip()
        await mabar(ctx, arg=arg)
        return

    # D) Forward gambar (konfirmasi)
    try:
        await _confirm_and_forward_images(message)
    except Exception as e:
        print("[WARN] forward images:", e)

    # E) Proses commands (ping, dw, downloader, announce, ...)
    await bot.process_commands(message)

    # F) Jika di private thread di bawah downloader → proses link apa saja
    if isinstance(message.channel, discord.Thread):
        parent = message.channel.parent
        if parent and parent.id == CHANNEL_ID_DOWNLOADER:
            url_m = URL_ANY.search(message.content or "")
            if not url_m:
                return
            role_light = message.guild.get_role(ROLE_ID_LIGHT) if message.guild else None
            if not role_light or role_light not in message.author.roles:
                await message.channel.send("❌ Hanya member dengan role 🔆 Light yang bisa memakai fitur ini.")
                return
            if not get_downloader_enabled(message.guild.id):
                await message.channel.send("⛔ Fitur downloader sedang non-aktif oleh admin.")
                return
            await process_download_in_thread(message.channel, message.author, url_m.group(1))

# =========================
# COMMANDS
# =========================
@bot.command()
async def ping(ctx: commands.Context):
    await ctx.send(f"Pong! {round(bot.latency * 1000)}ms")

# ---- Downloader switches (ADMIN) ----
@bot.command(name="downloader")
@commands.has_permissions(administrator=True)
async def downloader_cmd(ctx: commands.Context, mode: str):
    """!downloader on | off  (status disimpan di config/downloader)"""
    mode = mode.lower().strip()
    if ctx.channel.id != CHANNEL_ID_LOGS:
        return await ctx.send("Perintah ini hanya di channel moderator/log.", delete_after=8)
    if mode not in {"on", "off"}:
        return await ctx.send("Gunakan: `!downloader on` atau `!downloader off`", delete_after=8)

    set_downloader_status(mode == "on")
    await ctx.send(f"✅ Downloader di-{'aktifkan' if mode == 'on' else 'nonaktifkan'}.", delete_after=8)
    await ensure_downloader_notice()

# ---- Mulai sesi download privat ----
@bot.command(name="dw")
async def dw(ctx: commands.Context):
    """Mulai sesi download (buat thread privat). Pesan perintah dihapus setelah 30 detik."""
    if ctx.channel.id != CHANNEL_ID_DOWNLOADER:
        return await ctx.send(f"Fitur ini hanya di <#{CHANNEL_ID_DOWNLOADER}> ya.", delete_after=7)

    role_light = ctx.guild.get_role(ROLE_ID_LIGHT)
    if not role_light or role_light not in ctx.author.roles:
        return await ctx.send("❌ Hanya member dengan role 🔆 Light yang bisa memakai fitur ini.", delete_after=7)

    if not get_downloader_enabled(ctx.guild.id):
        return await ctx.send("⛔ Fitur downloader sedang non-aktif oleh admin.", delete_after=7)

    thread = await ensure_private_thread(ctx.channel, ctx.author)
    guide = (
        f"Hai {ctx.author.mention}! Kirim **tautan sosial (Instagram/TikTok/dll)** di thread ini.\n"
        "Thread ini **privat** (hanya kamu dan bot yang bisa melihat)."
    )
    await thread.send(guide, view=DlActionView(thread, ctx.author.id))

    try:
        await ctx.message.delete(delay=30)
    except Exception:
        pass

# ---------- ANNOUNCE (dari moderator/log ke server spotlight) ----------
@bot.command(name="announce")
@commands.has_permissions(manage_guild=True)
async def announce(ctx: commands.Context, *, text: str = ""):
    """
    Kirim pengumuman ke channel Server Spotlight.
    Pakai dari channel moderator/log saja.
    Opsi:
      --mention @everyone|@here|<@&ROLEID>
      --footer "teks footer"
    Attachment (gambar) akan di-embed (pertama sebagai image di embed).
    """
    if ctx.channel.id != CHANNEL_ID_LOGS:
        return await ctx.send("Perintah ini hanya di channel moderator/log.", delete_after=8)

    mention_val = None
    footer_val = None

    # Parse flags
    m_footer = re.search(r'--footer\s+"([^"]+)"', text) or re.search(r"--footer\s+'([^']+)'", text)
    if m_footer:
        footer_val = m_footer.group(1)
        text = text[:m_footer.start()] + text[m_footer.end():]

    m_mention = re.search(r"--mention\s+(\S+)", text)
    if m_mention:
        mention_val = m_mention.group(1)
        text = text[:m_mention.start()] + text[m_mention.end():]

    body = text.strip()
    if not body and not ctx.message.attachments:
        return await ctx.send("Tolong sertakan isi pengumuman atau lampiran.", delete_after=8)

    dest = bot.get_channel(CHANNEL_ID_SERVER_SPOTLIGHT)
    if not isinstance(dest, discord.TextChannel):
        return await ctx.send("Channel Server Spotlight tidak ditemukan.", delete_after=8)

    embed = discord.Embed(description=body or discord.Embed.Empty, color=discord.Color.gold())
    if footer_val:
        embed.set_footer(text=footer_val)

    image_set = False
    files_to_send = []
    for att in ctx.message.attachments[:4]:
        if not image_set and (att.content_type or "").lower().startswith("image/"):
            embed.set_image(url=att.url)
            image_set = True
        else:
            try:
                f = await att.to_file()
                files_to_send.append(f)
            except Exception:
                pass

    content_prefix = mention_val + "\n" if mention_val else None
    sent = await dest.send(content=content_prefix, embed=embed if (body or image_set) else None, files=files_to_send or None)

    # Log di Firestore
    log_announcement({
        "guild_id": ctx.guild.id,
        "from_channel_id": ctx.channel.id,
        "to_channel_id": dest.id,
        "author_id": ctx.author.id,
        "message_id": sent.id,
        "content": body,
        "footer": footer_val,
        "mention": mention_val,
        "attachments": [a.url for a in ctx.message.attachments] if ctx.message.attachments else [],
    })

    await ctx.send("✅ Pengumuman terkirim ke Server Spotlight.", delete_after=8)

# ---------- MABAR ----------
async def schedule_mabar_tasks_from_doc(doc_id: str, dat: dict):
    try:
        remind_at_epoch = float(dat["remind_at_epoch"])
        channel_id      = int(dat["channel_id"])
        map_name        = str(dat["map_name"])
        role_id         = int(dat.get("role_id", ROLE_ID_LIGHT))
        announce_msg_id = int(dat.get("announce_message_id", 0))
    except Exception as e:
        print("[WARN] Dokumen mabar invalid:", e, dat)
        return

    remind_at_dt = from_epoch_to_wib(remind_at_epoch)
    ch = bot.get_channel(channel_id)
    if not isinstance(ch, discord.TextChannel):
        print("[WARN] Channel mabar tidak ditemukan:", doc_id)
        return

    role_mention = f"<@&{role_id}>"

    async def remind_task():
        delay = max(0, (remind_at_dt - now_wib()).total_seconds())
        if delay > 60:
            await asyncio.sleep(delay)
        try:
            await ch.send(f"{role_mention}\n⏰ Waktunya mabar **{map_name.title()}**! Siap-siap yuk 🎮")
            update_mabar_status(doc_id, status="reminded")
        except Exception as e:
            print("[ERROR] Reminder gagal:", e)

    async def autodelete_task():
        total = max(0, (remind_at_dt - now_wib()).total_seconds()) + 3600
        await asyncio.sleep(total)
        if announce_msg_id:
            try:
                msg = await ch.fetch_message(announce_msg_id)
                await msg.delete()
            except Exception:
                pass
        update_mabar_status(doc_id, status="done")

    asyncio.create_task(remind_task())
    asyncio.create_task(autodelete_task())

@bot.command(aliases=["main"])
async def mabar(ctx: commands.Context, *, arg: str = None):
    """Manual trigger !main [map/game] [jam/waktu]"""
    if not arg:
        return await ctx.send("Gunakan format bebas: `!main [nama game/map] [jam/waktu]`")
    await handle_mabar_message(ctx, arg)

# Deteksi ajakan natural tanpa prefix
@bot.event
async def on_message_without_prefix(message: discord.Message):
    pass  # placeholder (kamu bisa mempertahankan versi deteksi natural bila perlu)

async def handle_mabar_message(ctx: commands.Context, text: str):
    role_light = ctx.guild.get_role(ROLE_ID_LIGHT) if ctx.guild else None
    if not role_light:
        return await ctx.send("⚠️ Role Light belum diset di kode.")
    if role_light not in ctx.author.roles:
        return await ctx.send("❌ Kamu belum punya role 🔆 Light untuk pakai perintah ini!")

    waktu_pattern = re.compile(
        r"(jam\s*\d{1,2}[:.]?\d{0,2}\s*(pagi|siang|sore|malam)?|besok|sekarang|skrng|skrg|now)",
        re.IGNORECASE
    )
    waktu_match = waktu_pattern.search(text)
    waktu_text = waktu_match.group(0) if waktu_match else "sekarang"

    if waktu_match:
        map_name = text[:waktu_match.start()].strip(" ,.!?")
    else:
        map_name = text.strip(" ,.!?")

    map_name = re.sub(
        r"\b(yok|ayo|ayok|gas|mabar|main|lets go|ditunggu|nih|ya|nanti|besok|ayo dong|yuk)\b",
        "",
        map_name,
        flags=re.IGNORECASE
    ).strip()

    ref = now_wib()
    waktu_text_norm = re.sub(r"[^a-z0-9:.\s]", "", waktu_text.lower()).strip()
    remind_at, when_str = parse_natural_time(waktu_text_norm or "sekarang", ref)

    embed = discord.Embed(
        title="🎮 Konfirmasi Mabar",
        description=(
            f"Game / Map: **{map_name.title() or 'Tidak disebut'}**\n"
            f"Waktu: **{when_str}**\n\n"
            f"Kirim pengumuman ke <#{CHANNEL_ID_MABAR}>?"
        ),
        color=discord.Color.purple()
    )
    msg = await ctx.send(embed=embed)
    for em in ("✅", "❌"):
        try:
            await msg.add_reaction(em)
        except Exception:
            pass

    def check(reaction, user):
        return (user == ctx.author and str(reaction.emoji) in ["✅", "❌"] and reaction.message.id == msg.id)

    try:
        reaction, _ = await bot.wait_for("reaction_add", timeout=60.0, check=check)
    except asyncio.TimeoutError:
        try: await msg.delete()
        except Exception: pass
        return await ctx.send("⏰ Waktu konfirmasi habis, mabar dibatalkan.", delete_after=5)

    if str(reaction.emoji) == "❌":
        try: await msg.delete()
        except Exception: pass
        return await ctx.send("❌ Mabar dibatalkan.", delete_after=5)

    try:
        await msg.delete()
    except Exception:
        pass

    mabar_channel = bot.get_channel(CHANNEL_ID_MABAR)
    if not isinstance(mabar_channel, discord.TextChannel):
        return await ctx.send("❌ Channel mabar tidak ditemukan.")

    announce_text = (
        f"{role_light.mention}\n"
        f"🎮 Yuk mabar **{map_name.title()}** jam **{when_str}**!"
    )
    announce_msg = await mabar_channel.send(announce_text)
    await ctx.send(f"✅ Pengumuman mabar dikirim ke <#{CHANNEL_ID_MABAR}>", delete_after=5)

    doc_id = f"{ctx.guild.id}-{announce_msg.id}"
    data = {
        "status": "scheduled",
        "guild_id": ctx.guild.id,
        "channel_id": CHANNEL_ID_MABAR,
        "role_id": ROLE_ID_LIGHT,
        "map_name": map_name,
        "announce_message_id": announce_msg.id,
        "created_by_id": ctx.author.id,
        "created_at": firestore.SERVER_TIMESTAMP,
        "remind_at_epoch": to_epoch(remind_at),
        "remind_at_wib": remind_at.strftime("%Y-%m-%d %H:%M:%S WIB"),
    }
    save_mabar_schedule(doc_id, data)
    await schedule_mabar_tasks_from_doc(doc_id, data)

# =========================
# RUN
# =========================
if __name__ == "__main__":
    try:
        bot.run(TOKEN)
    except discord.LoginFailure:
        print("❌ Token invalid. Pastikan DISCORD_BOT_TOKEN benar.")
    except Exception as e:
        print(f"[FATAL] Error menjalankan bot: {e}")
