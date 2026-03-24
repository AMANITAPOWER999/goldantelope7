import os, asyncio, re, uvicorn, difflib, time, json
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import MessageMediaPhoto

app = FastAPI()

API_ID = 32881984
API_HASH = 'd2588f09dfbc5103ef77ef21c07dbf8b'
SESS = os.environ.get('TELETHON_SESSION', '')

D = {
    'VIET': 'vietnamparsing',
    'THAI': 'thailandparsing',
    'BIKE': 'visaranvietnam',
    'MARKET': 'baraholkainvietnam',
    'FUN': 'razvlecheniyavietnam',
    'FOOD': 'restoranvietnam',
    'CHAT': 'obmenvietnam'
}

M = {
    'THAI': [
        'arenda_phukets','THAILAND_REAL_ESTATE_PHUKET','housephuket','arenda_phuket_thailand',
        'phuket_nedvizhimost_rent','phuketsk_arenda','phuket_nedvizhimost_thailand','phuketsk_for_rent',
        'phuket_rentas','rentalsphuketonli','rentbuyphuket','Phuket_thailand05','nedvizhimost_pattaya',
        'arenda_pattaya','pattaya_realty_estate','HappyHomePattaya','sea_bangkok','Samui_for_you',
        'sea_phuket','realty_in_thailand','nedvig_thailand','thailand_nedvizhimost','globe_nedvizhka_Thailand'
    ],
    'VIET': [
        'phuquoc_rent_wt','phyquocnedvigimost','Viet_Life_Phu_Quoc_rent','nhatrangapartment',
        'tanrealtorgh','viet_life_niachang','nychang_arenda','rent_nha_trang','nyachang_nedvizhimost',
        'nedvizimost_nhatrang','nhatrangforrent79','NhatrangRentl','arenda_v_nyachang','rent_appart_nha',
        'Arenda_Nyachang_Zhilye','NhaTrang_rental','realestatebythesea_1','NhaTrang_Luxury',
        'luckyhome_nhatrang','rentnhatrang','megasforrentnhatrang','viethome','gohomenhatrang',
        'Vietnam_arenda','huynhtruonq','DaNangRentAFlat','danag_viet_life_rent','Danang_House',
        'DaNangApartmentRent','danang_arenda','arenda_v_danang','HoChiMinhRentI','hcmc_arenda',
        'RentHoChiMinh','Hanoirentapartment','HanoiRentl','Hanoi_Rent','PhuquocRentl'
    ],
    'BIKE': [
        'bike_nhatrang','motohub_nhatrang','NhaTrang_moto_market','RentBikeUniq',
        'BK_rental','nha_trang_rent','RentTwentyTwo22NhaTrang'
    ],
    'MARKET': ['vietnam_poputchiki','danang_mart','baraholka_niachang'],
    'FUN': ['MelomaniaMusicNT'],
    'FOOD': ['vietnam_food','danang_food','food_muine','food_nhatrang','phuquoc_food'],
    'CHAT': [
        'vietnam_chatt','vungtau_chat','dalat_forum','danang_forum','danang_expats',
        'danang_woman','danang_chatik','kamran_chat','kuinen_chat','nhatrang_chatik',
        'nhatrang_expats','phanthiet_chat','fukuok_chatik','hochiminh_chat','hanoi_chat'
    ]
}

H = []
STATS = {
    'started_at': None,
    'user': None,
    'connected_channels': {},
    'failed_channels': {},
    'forwarded': {},
    'total_messages': 0,
    'total_photos': 0,
    'total_albums': 0,
    'session_mode': 'env' if SESS else 'none'
}

# Setup state (for in-space session generation)
SETUP = {'client': None, 'tmp_session': None, 'code_hash': None, 'phone': None}

def cl(t):
    if not t: return ""
    t = re.sub(r"t\.me/\S+|http\S+|#[A-Za-z0-9_а-яА-ЯёЁ]+|Источник:.*", "", t, flags=re.I)
    t = re.sub(r'[^\w\s.,!?:;()\-+=%№"\'/]', '', t)
    return " ".join(t.split())

def dup(t):
    if not t or len(t) < 20: return False
    c = re.sub(r'[^a-zA-Zа-яА-ЯёЁ0-9]', '', t)
    for o in H:
        if difflib.SequenceMatcher(None, c, o).ratio() > 0.88: return True
    H.append(c)
    if len(H) > 500: H.pop(0)
    return False

# ──── Setup endpoints (generate session FROM this server's IP) ────

class StartReq(BaseModel):
    phone: str

class VerifyReq(BaseModel):
    code: str

@app.post("/setup/start")
async def setup_start(req: StartReq):
    """Step 1: Send Telegram auth code to phone (called from HF Space IP)"""
    try:
        c = TelegramClient(StringSession(), API_ID, API_HASH)
        await c.connect()
        result = await c.send_code_request(req.phone)
        SETUP['client'] = c
        SETUP['tmp_session'] = c.session.save()
        SETUP['code_hash'] = result.phone_code_hash
        SETUP['phone'] = req.phone
        print(f"[SETUP] Код отправлен на {req.phone}")
        return {"ok": True, "message": f"Код отправлен на {req.phone}"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/setup/verify")
async def setup_verify(req: VerifyReq):
    """Step 2: Verify code and return session string"""
    if not SETUP['client'] or not SETUP['code_hash']:
        raise HTTPException(status_code=400, detail="Сначала вызовите /setup/start")
    try:
        c = SETUP['client']
        if not c.is_connected():
            await c.connect()
        await c.sign_in(SETUP['phone'], req.code, phone_code_hash=SETUP['code_hash'])
        me = await c.get_me()
        session_str = c.session.save()
        await c.disconnect()
        SETUP['client'] = None
        print(f"[SETUP] ✅ Сессия создана для {me.first_name}")
        return {
            "ok": True,
            "user": me.first_name,
            "session": session_str,
            "instruction": "Скопируйте 'session' и добавьте как секрет TELETHON_SESSION в настройках Space"
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# ──── Main parser ────

async def start_client():
    global STATS
    if not SESS:
        print("⚠️  TELETHON_SESSION не задана. Используйте /setup/start и /setup/verify для генерации сессии.")
        print("   Затем добавьте сессию как секрет TELETHON_SESSION и перезапустите Space.")
        return

    try:
        client = TelegramClient(StringSession(SESS), API_ID, API_HASH)
        await client.connect()

        if not await client.is_user_authorized():
            print("❌ Сессия невалидна или устарела!")
            return

        me = await client.get_me()
        STATS['started_at'] = time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())
        STATS['user'] = f"{me.first_name} (id={me.id})"
        print(f"\n✅ Авторизован: {me.first_name} | ID: {me.id}")

        all_ents = []
        print("\n📡 Подключение к каналам-источникам:")
        print("=" * 55)
        for grp, names in M.items():
            ok, fail = [], []
            for n in names:
                try:
                    ent = await client.get_input_entity(n)
                    all_ents.append(ent)
                    ok.append(n)
                except Exception as e:
                    fail.append(f"{n}({str(e)[:25]})")
            STATS['connected_channels'][grp] = ok
            STATS['failed_channels'][grp] = fail
            STATS['forwarded'][grp] = {'messages': 0, 'photos': 0, 'albums': 0}
            dest = D[grp]
            print(f"  [{grp}] → @{dest}: ✅{len(ok)}/{len(names)} подключено")
            if fail:
                print(f"    ❌ {', '.join(fail[:3])}{'...' if len(fail)>3 else ''}")

        total_ok = sum(len(v) for v in STATS['connected_channels'].values())
        total_all = sum(len(v) for v in M.values())
        print("=" * 55)
        print(f"📊 Итого: {total_ok}/{total_all} каналов | Слушаю...\n")

        @client.on(events.NewMessage(chats=all_ents))
        async def h(e):
            if e.grouped_id or not e.media or not isinstance(e.media, MessageMediaPhoto): return
            t = e.raw_text or ""
            if len(t) < 15 or dup(t): return
            try:
                chat = await e.get_chat()
                un = chat.username.lower() if hasattr(chat, 'username') and chat.username else str(e.chat_id)
                reg = next((r for r, l in M.items() if any(x.lower() == un for x in l)), 'VIET')
                msg = f"Источник: @{un}\nСсылка: https://t.me/{un}/{e.id}\n\n{cl(t)}"
                await client.send_message(D[reg], msg[:1020], file=e.media, parse_mode=None)
                STATS['forwarded'][reg]['messages'] += 1
                STATS['forwarded'][reg]['photos'] += 1
                STATS['total_messages'] += 1
                STATS['total_photos'] += 1
                print(f"📨 @{un}→@{D[reg]} | 📸1 | всего:{STATS['total_messages']} сообщ, {STATS['total_photos']} фото")
            except Exception as ex:
                print(f"⚠️  Ошибка: {ex}")

        @client.on(events.Album(chats=all_ents))
        async def ha(e):
            p = [m for m in e.messages if isinstance(m.media, MessageMediaPhoto)]
            if not p or dup(e.text): return
            try:
                chat = await e.get_chat()
                un = chat.username.lower() if hasattr(chat, 'username') and chat.username else str(e.chat_id)
                reg = next((r for r, l in M.items() if any(x.lower() == un for x in l)), 'VIET')
                msg = f"Источник: @{un}\nСсылка: https://t.me/{un}/{e.messages[0].id}\n\n{cl(e.text)}"
                await client.send_message(D[reg], msg[:1020], file=p, parse_mode=None)
                STATS['forwarded'][reg]['messages'] += 1
                STATS['forwarded'][reg]['photos'] += len(p)
                STATS['forwarded'][reg]['albums'] += 1
                STATS['total_messages'] += 1
                STATS['total_photos'] += len(p)
                STATS['total_albums'] += 1
                print(f"🖼️  @{un}→@{D[reg]} | 📸{len(p)} (альбом) | всего:{STATS['total_messages']} сообщ, {STATS['total_photos']} фото")
            except Exception as ex:
                print(f"⚠️  Ошибка альбом: {ex}")

        await client.run_until_disconnected()
    except Exception as ex:
        print(f"❌ Критическая ошибка клиента: {ex}")

@app.on_event("startup")
async def sup():
    asyncio.create_task(start_client())

@app.get("/")
async def root():
    return {"status": "ok", "parser": "globalparsing", "session": "configured" if SESS else "not_configured"}

@app.get("/stats")
async def stats():
    return STATS

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=7860)
