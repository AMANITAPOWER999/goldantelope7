import json
import os
import re
import time
import requests

BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')
CHANNEL = '@restoranvietnam'
PROGRESS_FILE = 'post_progress.json'
DELAY = 4  # seconds between posts


def clean_title(title):
    t = re.sub(r'^[\U0001F300-\U0001FFFF\u2600-\u26FF\u2700-\u27BF\s]+', '', title)
    t = re.sub(r'^РЕСТОРАН:\s*', '', t)
    t = re.sub(r'^НАЗВАНИЕ:\s*', '', t)
    t = re.sub(r'\s*сапфир.*', '', t, flags=re.IGNORECASE)
    t = re.sub(r'\[.*?\]|\(.*?\)', '', t)
    t = re.sub(r'\s{2,}', ' ', t).strip()
    return t


def load_restaurants():
    with open('listings_vietnam.json', encoding='utf-8') as f:
        data = json.load(f)
    result = []
    for item in data['restaurants']:
        if item['title'] == 'Channel created':
            continue
        desc = item.get('description', '')
        if len(desc) < 80:
            continue
        photos = item.get('photos') or item.get('images') or []
        if not photos:
            continue
        result.append({
            'id': item['id'],
            'title': clean_title(item['title']),
            'description': desc,
            'photos': photos[:10],
        })
    return result


def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {'posted_ids': []}


def save_progress(progress):
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f)


def download_photo(url):
    headers = {'User-Agent': 'Mozilla/5.0 (compatible; TelegramBot/1.0)'}
    try:
        r = requests.get(url, timeout=20, headers=headers)
        if r.status_code == 200:
            return r.content
    except Exception as e:
        print(f"    Download error: {e}")
    return None


def send_media_group_files(photos_data, caption):
    """Send album by uploading files directly"""
    url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendMediaGroup'
    files = {}
    media = []
    for i, img_bytes in enumerate(photos_data):
        attach_name = f'photo{i}'
        files[attach_name] = (f'photo{i}.jpg', img_bytes, 'image/jpeg')
        item = {'type': 'photo', 'media': f'attach://{attach_name}'}
        if i == 0:
            item['caption'] = caption[:1024]
            item['parse_mode'] = 'HTML'
        media.append(item)

    resp = requests.post(url, data={
        'chat_id': CHANNEL,
        'media': json.dumps(media)
    }, files=files, timeout=60)
    return resp.json()


def send_single_photo_file(img_bytes, caption):
    """Send single photo by uploading file directly"""
    url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto'
    resp = requests.post(url, data={
        'chat_id': CHANNEL,
        'caption': caption[:1024],
        'parse_mode': 'HTML'
    }, files={'photo': ('photo.jpg', img_bytes, 'image/jpeg')}, timeout=60)
    return resp.json()


def post_restaurant(r):
    caption = f"<b>🍽 {r['title']}</b>\n\n{r['description']}"
    photo_urls = r['photos']

    # Download photos
    photos_data = []
    for url in photo_urls:
        img = download_photo(url)
        if img:
            photos_data.append(img)

    if not photos_data:
        print(f"  ✗ No photos downloaded, skipping")
        return False

    if len(photos_data) == 1:
        result = send_single_photo_file(photos_data[0], caption)
    else:
        result = send_media_group_files(photos_data, caption)

    if result.get('ok'):
        return True
    else:
        err = result.get('description', 'unknown')
        print(f"  ✗ ERROR: {err}")
        if 'Too Many Requests' in err or 'retry after' in err.lower():
            wait_match = re.search(r'(\d+)', err)
            wait = int(wait_match.group(1)) + 3 if wait_match else 35
            print(f"  FloodWait: sleeping {wait}s...")
            time.sleep(wait)
            # Retry
            if len(photos_data) == 1:
                result2 = send_single_photo_file(photos_data[0], caption)
            else:
                result2 = send_media_group_files(photos_data, caption)
            if result2.get('ok'):
                return True
            print(f"  ✗ FAILED after retry: {result2.get('description')}")
        return False


def main():
    if not BOT_TOKEN:
        print('ERROR: TELEGRAM_BOT_TOKEN not set')
        return

    restaurants = load_restaurants()
    progress = load_progress()
    posted_ids = set(progress['posted_ids'])

    to_post = [r for r in restaurants if r['id'] not in posted_ids]
    print(f'Total: {len(restaurants)} | Already posted: {len(posted_ids)} | To post: {len(to_post)}')

    for i, r in enumerate(to_post):
        print(f"[{i+1}/{len(to_post)}] {r['title'][:50]}  ({len(r['photos'])} photos)", flush=True)

        ok = post_restaurant(r)
        if ok:
            posted_ids.add(r['id'])
            progress['posted_ids'] = list(posted_ids)
            save_progress(progress)
            print(f"  ✓ OK", flush=True)
        else:
            print(f"  → Skipped", flush=True)

        time.sleep(DELAY)

    print(f"\nDone! Posted {len(posted_ids)} total.")


if __name__ == '__main__':
    main()
