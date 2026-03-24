import os, asyncio, re, uvicorn, difflib, time
from fastapi import FastAPI
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
    'started_at': time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime()),
    'connected_channels': {},
    'failed_channels': {},
    'forwarded': {},
    'total_messages': 0,
    'total_photos': 0,
    'total_albums': 0,
}

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

async def start_client():
    if not SESS:
        print("❌ TELETHON_SESSION не задана!")
        return
    try:
        client = TelegramClient(StringSession(SESS), API_ID, API_HASH)
        await client.connect()

        if not await client.is_user_authorized():
            print("❌ Сессия невалидна!")
            return

        me = await client.get_me()
        print(f"✅ Авторизован: {me.first_name} | ID: {me.id}")
        STATS['user'] = f"{me.first_name} (id={me.id})"

        all_ents = []
        print("\n📡 Подключение к каналам-источникам:")
        print("=" * 50)
        for grp, names in M.items():
            ok, fail = [], []
            for n in names:
                try:
                    ent = await client.get_input_entity(n)
                    all_ents.append(ent)
                    ok.append(n)
                except Exception as e:
                    fail.append(f"{n}({str(e)[:30]})")
            STATS['connected_channels'][grp] = ok
            STATS['failed_channels'][grp] = fail
            STATS['forwarded'][grp] = {'messages': 0, 'photos': 0, 'albums': 0}
            dest = D[grp]
            print(f"  [{grp}] → @{dest}: ✅{len(ok)}/{len(names)} каналов подключено")
            if fail:
                print(f"    ❌ Недоступны: {', '.join(fail[:3])}{'...' if len(fail)>3 else ''}")

        total_connected = sum(len(v) for v in STATS['connected_channels'].values())
        total_channels = sum(len(v) for v in M.values())
        print("=" * 50)
        print(f"📊 Итого: {total_connected}/{total_channels} каналов | Слушаю сообщения...\n")

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
                print(f"📨 @{un} → @{D[reg]} | 📸1 фото | Итого: {STATS['total_messages']} сообщ.")
            except Exception as ex:
                print(f"⚠️ Ошибка пересылки: {ex}")

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
                print(f"🖼️  @{un} → @{D[reg]} | 📸{len(p)} фото (альбом) | Итого: {STATS['total_messages']} сообщ.")
            except Exception as ex:
                print(f"⚠️ Ошибка пересылки альбома: {ex}")

        await client.run_until_disconnected()
    except Exception as ex:
        print(f"❌ Критическая ошибка: {ex}")

@app.on_event("startup")
async def sup():
    asyncio.create_task(start_client())

@app.get("/")
async def root():
    return {"status": "ok", "parser": "globalparsing"}

@app.get("/stats")
async def stats():
    return STATS

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=7860)
